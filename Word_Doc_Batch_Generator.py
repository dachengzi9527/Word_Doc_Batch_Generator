import json
import os
import sys
import threading
import logging
import copy
from functools import partial

import pandas as pd

# 配置日志记录
try:
    # 使用 delay=True 避免初始化日志文件时的锁问题
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('word_batch_generator.log', encoding='utf-8', delay=True),
            logging.StreamHandler()
        ]
    )
    logger = logging.getLogger(__name__)
except Exception as e:
    # 如果日志文件无法写入，只使用控制台输出
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )
    logger = logging.getLogger(__name__)
    logger.warning(f"无法初始化日志文件: {str(e)}，仅使用控制台输出")
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QPushButton, QVBoxLayout, QFileDialog,
    QLabel, QHBoxLayout, QWidget, QMessageBox, QScrollArea, QComboBox, QDialog, QGridLayout, QSplitter, QCheckBox, QLineEdit
)
from docxtpl import DocxTemplate
from PySide6.QtCore import Qt, QObject, Signal, QThread


class DocumentGeneratorThread(QThread):
    """文档生成线程，继承自QThread"""
    progress_updated = Signal(int, str)  # 进度值, 状态信息
    finished = Signal(bool, str)  # 是否成功, 结果信息
    
    def __init__(self, parent, folder_field, file_field, ignore_missing, output_path, excel_data, word_template_path, template_mapping, cancel_event):
        super().__init__(parent)
        self.folder_field = folder_field
        self.file_field = file_field
        self.ignore_missing = ignore_missing
        self.output_path = output_path
        self.excel_data = excel_data
        self.word_template_path = word_template_path
        self.template_mapping = template_mapping
        self.cancel_event = cancel_event
    
    def run(self):
        """线程执行的主函数，管理文档生成过程"""
        import logging
        import os
        from document_generator import DocumentGenerator
        
        logger = logging.getLogger(__name__)
        logger.info("开始执行文档生成工作线程")
        
        try:
            # 一次性将Excel数据转换为字典列表，提高遍历性能
            # 避免在循环中反复使用 getattr / row.xxx
            rows = [
                row._asdict()
                for row in self.excel_data.itertuples(index=False, name="Row")
            ]
            
            # 定义进度回调函数
            def progress_callback(progress, message):
                self.progress_updated.emit(progress, message)
            
            # 创建DocumentGenerator实例
            generator = DocumentGenerator()
            
            # 调用DocumentGenerator.generate()方法执行核心生成逻辑
            successful_count, error_messages, exception = generator.generate(
                rows=rows, 
                folder_field=self.folder_field,
                file_field=self.file_field,
                ignore_missing=self.ignore_missing,
                output_path=self.output_path,
                word_template_path=self.word_template_path,
                template_mapping=self.template_mapping,
                cancel_event=self.cancel_event,
                progress_cb=progress_callback
            )
            
            # 检查是否有致命异常
            if exception:
                logger.critical(f"生成文档时发生致命错误: {str(exception)}")
                self.finished.emit(False, f"生成文档时出错: {str(exception)}")
                return
            
            # 处理生成结果
            if successful_count > 0:
                try:
                    # 打开输出文件夹
                    os.startfile(self.output_path)
                except Exception as e:
                    # 忽略打开文件夹的错误，不影响生成结果
                    logger.warning(f"无法打开输出文件夹: {str(e)}")
                    
                result_message = f"文档批量生成完成！\n成功生成: {successful_count} 个文档"
                if error_messages:
                    result_message += f"\n\n错误详情:\n" + "\n".join(error_messages[:5])
                    if len(error_messages) > 5:
                        result_message += f"\n... 还有 {len(error_messages) - 5} 个错误"
                logger.info(f"批量生成完成，成功生成 {successful_count} 个文档，失败 {len(error_messages)} 个")
                self.finished.emit(True, result_message)
            else:
                logger.error(f"所有文档生成失败！错误详情: {error_messages}")
                self.finished.emit(False, f"所有文档生成失败！\n错误详情:\n" + "\n".join(error_messages))
                
        except Exception as e:
            logger.critical(f"生成文档时发生致命错误: {str(e)}")
            self.finished.emit(False, f"生成文档时出错: {str(e)}")


class StyledButton(QPushButton):
    def __init__(self, text, tooltip=""):
        super().__init__(text)
        self.setToolTip(tooltip)
        self.setMinimumHeight(40)
        self.setCursor(Qt.PointingHandCursor)


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Word批量生成工具")
        self.setGeometry(100, 100, 1200, 900)
        logger.info("Word批量生成工具启动")

        # 初始化数据属性
        self.excel_path = None
        self.word_template_path = None
        self.output_path = None
        self.excel_data = None
        self.template_mapping = {}
        self.cancel_event = threading.Event()  # 取消生成事件


        # 初始化UI组件
        self.init_ui_components()
        self.setup_main_layout()
        
    def init_ui_components(self):
        """初始化所有UI组件"""
        
        # 创建按钮组件
        button_configs = [
            ('import_excel_btn', "导入 Excel 模板", "选择包含数据的Excel文件"),
            ('import_word_btn', "导入 Word 模板", "选择基础Word模板文件"),
            ('output_path_btn', "设置输出路径", "指定生成文件的保存位置"),
            ('open_word_btn', "打开 Word 模板", "预览当前选择的Word模板"),
            ('configure_template_btn', "配置模板", "设置字段与模板的对应关系"),
            ('batch_generate_btn', "批量生成文档", "开始生成所有Word文档"),
            ('cancel_btn', "取消生成", "取消正在进行的文档生成操作")
        ]
        
        for attr, text, tooltip in button_configs:
            btn = StyledButton(text, tooltip)
            setattr(self, attr, btn)

        # 创建下拉框和标签
        self.folder_field_combo = QComboBox()
        self.file_field_combo = QComboBox()
        self.folder_field_label = QLabel("选择文件夹名字段:")
        self.file_field_label = QLabel("选择文件命名字段:")
        
        # 添加忽略缺失占位符的复选框
        self.ignore_missing_placeholders_checkbox = QCheckBox("忽略缺失占位符")
        self.ignore_missing_placeholders_checkbox.setToolTip("勾选后，生成文档时将忽略Excel数据中不存在的占位符")

        # 创建路径显示标签
        path_labels = {
            'excel_path_label': "Excel 文件路径: 未选择",
            'word_path_label': "Word 文件路径: 未选择",
            'output_path_label': "输出路径: 未设置"
        }
        for attr, text in path_labels.items():
            lbl = QLabel(text)
            lbl.setWordWrap(True)
            setattr(self, attr, lbl)

        # 添加进度显示组件
        from PySide6.QtWidgets import QProgressBar
        self.progress_bar = QProgressBar()
        self.progress_bar.setValue(0)
        self.progress_bar.setMaximum(100)
        self.progress_bar.setVisible(False)  # 默认隐藏

        # 初始化字段显示区域布局
        self.fields_area = QScrollArea()
        self.fields_widget = QWidget()
        self.fields_layout = QVBoxLayout()
        self.fields_widget.setLayout(self.fields_layout)
        self.fields_area.setWidget(self.fields_widget)
        self.fields_area.setWidgetResizable(True)
        
    def setup_main_layout(self):
        """设置主窗口的布局结构"""
        
        # 创建主窗口的中心部件
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        # 创建主布局
        main_layout = QVBoxLayout(central_widget)
        
        # 创建按钮布局
        button_layout = QHBoxLayout()
        button_layout.addWidget(self.import_excel_btn)
        button_layout.addWidget(self.import_word_btn)
        button_layout.addWidget(self.output_path_btn)
        button_layout.addWidget(self.open_word_btn)
        button_layout.addWidget(self.configure_template_btn)
        button_layout.addWidget(self.batch_generate_btn)
        button_layout.addWidget(self.cancel_btn)
        
        # 创建路径信息布局
        path_layout = QVBoxLayout()
        path_layout.addWidget(self.excel_path_label)
        path_layout.addWidget(self.word_path_label)
        path_layout.addWidget(self.output_path_label)
        
        # 创建字段选择布局
        field_layout = QHBoxLayout()
        field_layout.addWidget(self.folder_field_label)
        field_layout.addWidget(self.folder_field_combo)
        field_layout.addWidget(self.file_field_label)
        field_layout.addWidget(self.file_field_combo)
        field_layout.addWidget(self.ignore_missing_placeholders_checkbox)
        
        # 将所有布局添加到主布局
        main_layout.addLayout(button_layout)
        main_layout.addLayout(path_layout)
        main_layout.addLayout(field_layout)
        main_layout.addWidget(self.progress_bar)
        main_layout.addWidget(self.fields_area)
        
        logger.info("主窗口布局设置完成")
        
        # 连接按钮信号和槽
        self.connect_buttons()
        
    def connect_buttons(self):
        """连接按钮的点击信号到对应的处理方法"""
        self.import_excel_btn.clicked.connect(self.import_excel_btn_clicked)
        self.import_word_btn.clicked.connect(self.import_word_btn_clicked)
        self.output_path_btn.clicked.connect(self.output_path_btn_clicked)
        self.open_word_btn.clicked.connect(self.open_word_btn_clicked)
        self.configure_template_btn.clicked.connect(self.configure_template_btn_clicked)
        self.batch_generate_btn.clicked.connect(self.batch_generate_btn_clicked)
        self.cancel_btn.clicked.connect(self.cancel_btn_clicked)
        
    def import_excel_btn_clicked(self):
        """处理导入Excel文件按钮点击事件"""
        logger.info("导入Excel按钮被点击")
        
        # 打开文件选择对话框
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "选择Excel文件",
            "",
            "Excel Files (*.xlsx *.xls)"
        )
        
        if file_path:
            try:
                logger.info(f"选择了Excel文件: {file_path}")
                
                # 读取Excel文件数据，将所有列读取为字符串类型，保持原始格式
                self.excel_data = pd.read_excel(file_path, dtype=str)
                self.excel_path = file_path
                
                # 更新路径标签
                self.excel_path_label.setText(f"Excel 文件路径: {file_path}")
                
                # 更新下拉框选项
                self.update_field_combos()
                
                # 更新字段展示区域
                self.update_fields_display()
                
                # 显示成功消息
                QMessageBox.information(self, "成功", "Excel文件导入成功！")
                
            except Exception as e:
                logger.error(f"导入Excel文件失败: {str(e)}")
                QMessageBox.critical(self, "错误", f"导入Excel文件失败: {str(e)}")
                
    def update_field_combos(self):
        """更新文件夹名和文件名选择下拉框的选项"""
        if self.excel_data is not None:
            # 获取Excel的列名
            columns = list(self.excel_data.columns)
            
            # 清空下拉框
            self.folder_field_combo.clear()
            self.file_field_combo.clear()
            
            # 添加选项
            self.folder_field_combo.addItems(columns)
            self.file_field_combo.addItems(columns)
            
            logger.info(f"更新了字段选择下拉框，可用字段: {columns}")
    
    def update_fields_display(self):
        """更新字段展示区域，显示所有字段名及其对应的替换关键词"""
        logger.info("更新字段展示区域")
        
        # 清空当前的字段展示区域
        for i in reversed(range(self.fields_layout.count())):
            widget = self.fields_layout.itemAt(i).widget()
            if widget is not None:
                widget.deleteLater()
        
        if self.excel_data is not None:
            # 获取Excel的列名
            columns = list(self.excel_data.columns)
            
            # 创建标题标签
            title_label = QLabel("字段名与替换关键词对应关系")
            title_label.setStyleSheet("font-weight: bold; font-size: 12pt; margin-bottom: 10px;")
            self.fields_layout.addWidget(title_label)
            
            # 为每个字段创建显示控件
            for column in columns:
                # 创建字段显示控件
                field_widget = QWidget()
                field_layout = QHBoxLayout(field_widget)
                field_layout.setContentsMargins(0, 5, 0, 5)
                
                # 创建字段名标签
                field_name_label = QLabel(f"字段名: {column}")
                field_name_label.setFixedWidth(150)
                field_name_label.setStyleSheet("font-weight: bold;")
                
                # 创建箭头标签
                arrow_label = QLabel("→")
                arrow_label.setFixedWidth(50)
                arrow_label.setAlignment(Qt.AlignCenter)
                
                # 创建替换关键词标签
                replace_key_label = QLabel(f"替换关键词: {{{{{column}}}}}")
                replace_key_label.setStyleSheet("color: #0066cc;")
                
                # 创建复制按钮
                copy_btn = QPushButton("复制")
                copy_btn.setFixedWidth(60)
                copy_btn.clicked.connect(lambda checked, key=f"{{{{{column}}}}}": self.copy_to_clipboard(key))
                
                # 将控件添加到水平布局
                field_layout.addWidget(field_name_label)
                field_layout.addWidget(arrow_label)
                field_layout.addWidget(replace_key_label)
                field_layout.addWidget(copy_btn)
                field_layout.addStretch()
                
                # 将水平布局控件添加到垂直布局
                self.fields_layout.addWidget(field_widget)
            
            logger.info(f"已在字段展示区域显示{len(columns)}个字段")
        else:
            # 如果没有导入Excel文件，显示提示信息
            info_label = QLabel("请先导入Excel文件以查看字段信息")
            info_label.setAlignment(Qt.AlignCenter)
            info_label.setStyleSheet("color: #666666;")
            self.fields_layout.addWidget(info_label)
            
            logger.info("未导入Excel文件，字段展示区域显示提示信息")
    
    def copy_to_clipboard(self, text):
        """将文本复制到剪贴板"""
        clipboard = QApplication.clipboard()
        clipboard.setText(text)
        QMessageBox.information(self, "成功", f"已将'{text}'复制到剪贴板！")
        logger.info(f"已将文本'{text}'复制到剪贴板")
        
    def import_word_btn_clicked(self):
        """处理导入Word模板按钮点击事件"""
        logger.info("导入Word模板按钮被点击")
        
        # 打开文件选择对话框
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "选择Word模板文件",
            "",
            "Word Files (*.docx)"
        )
        
        if file_path:
            try:
                logger.info(f"选择了Word模板文件: {file_path}")
                
                # 保存Word模板路径
                self.word_template_path = file_path
                
                # 更新路径标签
                self.word_path_label.setText(f"Word 文件路径: {file_path}")
                
                # 显示成功消息
                QMessageBox.information(self, "成功", "Word模板文件导入成功！")
                
            except Exception as e:
                logger.error(f"导入Word模板文件失败: {str(e)}")
                QMessageBox.critical(self, "错误", f"导入Word模板文件失败: {str(e)}")
        
    def output_path_btn_clicked(self):
        """处理设置输出路径按钮点击事件"""
        logger.info("设置输出路径按钮被点击")
        
        # 打开文件夹选择对话框
        folder_path = QFileDialog.getExistingDirectory(
            self,
            "选择输出路径",
            ""
        )
        
        if folder_path:
            try:
                logger.info(f"选择了输出路径: {folder_path}")
                
                # 保存输出路径
                self.output_path = folder_path
                
                # 更新路径标签
                self.output_path_label.setText(f"输出路径: {folder_path}")
                
                # 显示成功消息
                QMessageBox.information(self, "成功", "输出路径设置成功！")
                
            except Exception as e:
                logger.error(f"设置输出路径失败: {str(e)}")
                QMessageBox.critical(self, "错误", f"设置输出路径失败: {str(e)}")
        
    def open_word_btn_clicked(self):
        """处理打开Word模板按钮点击事件"""
        logger.info("打开Word模板按钮被点击")
        
        if self.word_template_path:
            try:
                logger.info(f"打开Word模板文件: {self.word_template_path}")
                
                # 使用系统默认程序打开Word模板
                if os.path.exists(self.word_template_path):
                    os.startfile(self.word_template_path)
                    QMessageBox.information(self, "提示", "正在打开Word模板文件...")
                else:
                    QMessageBox.warning(self, "警告", "Word模板文件不存在！")
                    
            except Exception as e:
                logger.error(f"打开Word模板文件失败: {str(e)}")
                QMessageBox.critical(self, "错误", f"打开Word模板文件失败: {str(e)}")
        else:
            QMessageBox.warning(self, "警告", "请先导入Word模板文件！")
        
    def configure_template_btn_clicked(self):
        """处理配置模板按钮点击事件"""
        logger.info("配置模板按钮被点击")
        
        # 检查是否已经导入了Excel文件
        if not self.excel_path:
            QMessageBox.warning(self, "警告", "请先导入Excel文件！")
            return
        
        try:
            # 创建模板配置对话框
            dlg = TemplateConfigDialog(self.excel_data, self.template_mapping, self)
            
            if dlg.exec() == QDialog.Accepted:
                self.template_mapping = dlg.get_mapping()
                logger.info(f"模板映射配置已更新: {self.template_mapping}")
                QMessageBox.information(self, "成功", "模板配置完成！")
                
        except Exception as e:
            logger.error(f"模板配置失败: {str(e)}")
            QMessageBox.critical(self, "错误", f"模板配置失败: {str(e)}")
        
    def batch_generate_btn_clicked(self):
        """处理批量生成文档按钮点击事件"""
        logger.info("批量生成文档按钮被点击")
        
        # 检查是否已经导入了必要的文件和配置
        if not self.excel_path:
            QMessageBox.warning(self, "警告", "请先导入Excel文件！")
            return
        
        if not self.word_template_path:
            QMessageBox.warning(self, "警告", "请先导入Word模板文件！")
            return
        
        if not self.output_path:
            QMessageBox.warning(self, "警告", "请先设置输出路径！")
            return
        
        # 检查是否选择了文件夹名和文件名字段
        if self.folder_field_combo.currentText() == "":
            QMessageBox.warning(self, "警告", "请选择文件夹名字段！")
            return
        
        if self.file_field_combo.currentText() == "":
            QMessageBox.warning(self, "警告", "请选择文件名字段！")
            return
        
        try:
            # 设置取消事件
            self.cancel_event.clear()
            
            # 获取批量生成参数
            folder_field = self.folder_field_combo.currentText()
            file_field = self.file_field_combo.currentText()
            ignore_missing = self.ignore_missing_placeholders_checkbox.isChecked()
            
            logger.info(f"开始批量生成文档，参数: 文件夹字段={folder_field}, 文件名字段={file_field}, 忽略缺失={ignore_missing}")
            
            # 创建文档生成线程
            self.generator_thread = DocumentGeneratorThread(
                self,
                folder_field,
                file_field,
                ignore_missing,
                self.output_path,
                self.excel_data,
                self.word_template_path,
                self.template_mapping,
                self.cancel_event
            )
            
            # 连接线程信号
            self.generator_thread.progress_updated.connect(self.on_progress_updated)
            self.generator_thread.finished.connect(self.on_generate_finished)
            
            # 显示进度条
            self.progress_bar.setVisible(True)
            self.progress_bar.setValue(0)
            
            # 禁用相关按钮
            self.batch_generate_btn.setEnabled(False)
            self.cancel_btn.setEnabled(True)
            
            # 启动线程
            self.generator_thread.start()
            
        except Exception as e:
            logger.error(f"批量生成文档失败: {str(e)}")
            QMessageBox.critical(self, "错误", f"批量生成文档失败: {str(e)}")
        
    def cancel_btn_clicked(self):
        """处理取消生成按钮点击事件"""
        logger.info("取消生成按钮被点击")
        
        if hasattr(self, 'generator_thread') and self.generator_thread.isRunning():
            self.cancel_event.set()
            QMessageBox.information(self, "提示", "正在取消生成任务...")
        else:
            QMessageBox.information(self, "提示", "当前没有正在进行的生成任务")
            
    def on_progress_updated(self, progress, message):
        """处理进度更新信号"""
        self.progress_bar.setValue(progress)
        logger.info(f"生成进度: {progress}% - {message}")
        
    def on_generate_finished(self, success, message):
        """处理生成完成信号"""
        # 重置进度条
        self.progress_bar.setValue(100)
        self.progress_bar.setVisible(False)
        
        # 启用相关按钮
        self.batch_generate_btn.setEnabled(True)
        self.cancel_btn.setEnabled(False)
        
        # 显示结果消息
        if success:
            QMessageBox.information(self, "成功", message)
        else:
            QMessageBox.critical(self, "错误", message)
    
    def _select_file(self, caption, filter):
        """选择文件的辅助方法"""
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            caption,
            "",
            filter
        )
        return file_path if file_path else None
    
    def load_config(self, config_path=None):
        """从文件加载配置，返回(success, result)元组"""
        if config_path is None:
            config_path = "config.json"
        
        try:
            if not os.path.exists(config_path):
                return False, "配置文件不存在"
                
            if not os.access(config_path, os.R_OK):
                return False, f"对配置文件没有读取权限: {config_path}"
                
            with open(config_path, "r", encoding="utf-8") as file:
                config = json.load(file)
                # 验证配置格式
                if isinstance(config, dict):
                    return True, config
                else:
                    return False, f"配置文件格式不正确: {config_path}"
        except json.JSONDecodeError as e:
            return False, f"配置文件格式错误，无法解析: {str(e)}"
        except UnicodeDecodeError:
            return False, f"配置文件编码错误，无法读取: {config_path}"
        except Exception as e:
            return False, f"加载配置文件时出错: {str(e)}"
    
    def save_config(self, config, config_path=None):
        """保存配置到文件，返回(success, message)元组"""
        if config_path is None:
            config_path = "config.json"
        
        try:
            # 验证配置数据类型
            if not isinstance(config, dict):
                return False, "配置数据必须是字典类型"
                
            # 检查配置文件目录是否可写
            config_dir = os.path.dirname(config_path)
            if config_dir and not os.access(config_dir, os.W_OK):
                return False, f"对配置文件目录没有写入权限: {config_dir}"
                
            # 检查是否有权限写入文件
            if os.path.exists(config_path) and not os.access(config_path, os.W_OK):
                return False, f"对配置文件没有写入权限: {config_path}"
                
            # 写入配置文件
            with open(config_path, "w", encoding="utf-8") as file:
                json.dump(config, file, ensure_ascii=False, indent=4)
                
            return True, f"配置已成功保存到 {config_path}"
        except IOError as e:
            return False, f"写入配置文件时出错: {str(e)}"
        except Exception as e:
            return False, f"保存配置文件时出错: {str(e)}"
            

class TemplateConfigDialog(QDialog):
    """模板配置对话框"""
    def __init__(self, excel_data, current_mapping, parent=None):
        super().__init__(parent)
        self.setWindowTitle("配置模板")
        self.setMinimumSize(800, 600)
        
        self.excel_data = excel_data
        self.current_mapping = current_mapping or {}
        
        # 用于存储所有字段的模板映射
        self.field_template_mapping = copy.deepcopy(self.current_mapping)
        
        self.init_ui()
        
    def init_ui(self):
        """初始化对话框UI"""
        layout = QVBoxLayout(self)

        # 字段选择下拉框
        field_label = QLabel("选择字段：")
        self.field_combo = QComboBox()
        self.field_combo.addItems(self.excel_data.columns.tolist())
        self.field_combo.currentTextChanged.connect(self.load_field_values)
        layout.addWidget(field_label)
        layout.addWidget(self.field_combo)

        # 字段值与模板配置区域
        self.value_scroll_area = QScrollArea()
        self.value_widget = QWidget()
        self.value_layout = QVBoxLayout()
        self.value_widget.setLayout(self.value_layout)
        self.value_scroll_area.setWidget(self.value_widget)
        self.value_scroll_area.setWidgetResizable(True)
        layout.addWidget(self.value_scroll_area)

        # 工具栏
        toolbar = QHBoxLayout()
        load_btn = QPushButton("加载配置")
        save_btn = QPushButton("保存配置")
        import_btn = QPushButton("导入配置")
        export_btn = QPushButton("导出配置")
        
        load_btn.clicked.connect(self.load_config)
        save_btn.clicked.connect(self.save_config)
        import_btn.clicked.connect(self.import_config)
        export_btn.clicked.connect(self.export_config)
        
        toolbar.addWidget(load_btn)
        toolbar.addWidget(save_btn)
        toolbar.addWidget(import_btn)
        toolbar.addWidget(export_btn)
        layout.addLayout(toolbar)

        # 底部按钮
        button_layout = QHBoxLayout()
        ok_button = QPushButton("确定")
        cancel_button = QPushButton("取消")
        
        ok_button.clicked.connect(self.accept)
        cancel_button.clicked.connect(self.reject)
        
        button_layout.addStretch()
        button_layout.addWidget(ok_button)
        button_layout.addWidget(cancel_button)
        
        layout.addLayout(button_layout)

    def load_field_values(self):
        """加载当前字段的所有值并显示配置界面"""
        # 清空当前值配置
        for i in reversed(range(self.value_layout.count())):
            widget = self.value_layout.itemAt(i).widget()
            if widget is not None:
                widget.deleteLater()

        # 获取当前选中的字段
        selected_field = self.field_combo.currentText()
        if not selected_field:
            return

        # 如果之前已经有这个字段的映射，先显示出来
        field_mapping = self.field_template_mapping.get(selected_field, {})

        # 加载选定字段的值
        if selected_field not in self.excel_data.columns:
            QMessageBox.warning(self, "字段错误", f"Excel 中不存在字段: {selected_field}")
            return
            
        unique_values = self.excel_data[selected_field].dropna().unique()
        if len(unique_values) == 0:
            QMessageBox.information(self, "提示", f"字段 '{selected_field}' 没有可用值")
            return

        for value in unique_values:
            value_layout_row = QHBoxLayout()
            value_label = QLabel(f"值: {value}")
            select_path_button = QPushButton("选择模板路径")

            # 如果已经有这个值的模板，显示模板路径
            select_path_label = QLabel(field_mapping.get(value, "未选择模板"))

            def handle_choose_template(curr_field, curr_value, label):
                try:
                    path = self.parent()._select_file(
                        f"选择字段 '{curr_field}' 值 '{curr_value}' 的模板",
                        "Word 文件 (*.docx)"
                    )
                    if path:
                        # 验证模板文件
                        if not os.path.exists(path):
                            QMessageBox.warning(self, "文件不存在", f"选择的模板文件不存在: {path}")
                            return
                            
                        if not path.lower().endswith('.docx'):
                            QMessageBox.warning(self, "文件格式错误", f"选择的文件不是有效的Word文档: {path}")
                            return
                            
                        # 保存模板路径
                        if curr_field not in self.field_template_mapping:
                            self.field_template_mapping[curr_field] = {}
                        self.field_template_mapping[curr_field][curr_value] = path
                        label.setText(path)
                except Exception as e:
                    QMessageBox.critical(self, "选择模板错误", f"选择模板文件时出错: {str(e)}")

            select_path_button.clicked.connect(lambda checked, f=selected_field, v=value, l=select_path_label: handle_choose_template(f, v, l))

            value_layout_row.addWidget(value_label)
            value_layout_row.addStretch()
            value_layout_row.addWidget(select_path_button)
            value_layout_row.addWidget(select_path_label)
            self.value_layout.addLayout(value_layout_row)

    def load_config(self):
        """加载配置文件"""
        try:
            success, result = self.parent().load_config()
            if success:
                self.field_template_mapping = result
                QMessageBox.information(self, "提示", "配置文件加载成功！")
                self.load_field_values()  # 刷新字段值显示
            else:
                QMessageBox.warning(self, "错误", result)
        except Exception as e:
            QMessageBox.critical(self, "配置加载错误", f"加载配置文件时出错: {str(e)}")

    def save_config(self):
        """保存配置文件"""
        try:
            success, message = self.parent().save_config(self.field_template_mapping)
            if success:
                QMessageBox.information(self, "提示", f"配置已保存到 config.json！")
            else:
                QMessageBox.warning(self, "保存失败", message)
        except Exception as e:
            QMessageBox.critical(self, "配置保存错误", f"保存配置文件时出错: {str(e)}")

    def import_config(self):
        """导入配置文件"""
        try:
            config_path = self.parent()._select_file("导入配置文件", "JSON 文件 (*.json)")
            if config_path:
                success, result = self.parent().load_config(config_path)
                if success:
                    self.field_template_mapping = result
                    QMessageBox.information(self, "提示", "配置文件导入成功！")
                    self.load_field_values()
                else:
                    QMessageBox.warning(self, "导入失败", result)
        except Exception as e:
            QMessageBox.critical(self, "配置导入错误", f"导入配置文件时出错: {str(e)}")

    def export_config(self):
        """导出配置文件"""
        try:
            file_dialog = QFileDialog()
            config_path, _ = file_dialog.getSaveFileName(
                self, "导出配置文件", "config_export.json", "JSON 文件 (*.json)"
            )
            if config_path:
                if not config_path.lower().endswith('.json'):
                    config_path += '.json'
                success, message = self.parent().save_config(self.field_template_mapping, config_path)
                if success:
                    QMessageBox.information(self, "提示", f"配置已导出到 {config_path}！")
                else:
                    QMessageBox.warning(self, "导出失败", message)
        except Exception as e:
            QMessageBox.critical(self, "配置导出错误", f"导出配置文件时出错: {str(e)}")

    def get_mapping(self):
        """获取配置的字段映射"""
        return self.field_template_mapping


# 应用程序入口点
if __name__ == "__main__":
    # 创建QApplication实例
    app = QApplication(sys.argv)
    
    # 创建主窗口实例
    window = MainWindow()
    
    # 显示窗口
    window.show()
    
    # 运行应用程序事件循环
    sys.exit(app.exec())
