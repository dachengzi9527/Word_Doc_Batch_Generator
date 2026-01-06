# document_generator.py
import os
import io
import time
import copy
import logging
import re
from concurrent.futures import ProcessPoolExecutor, as_completed
from docxtpl import DocxTemplate

logger = logging.getLogger(__name__)


def _sanitize_filename(filename, default_name):
    """清理文件名中的非法字符，并确保文件名不为空"""
    sanitized = str(filename).strip()
    sanitized = re.sub(r'[<>:"/\\|?*]+', '_', sanitized)
    if not sanitized:
        sanitized = default_name
    return sanitized


def _unique_file_path(file_path):
    """生成唯一的文件路径，如果文件已存在则添加数字后缀"""
    if not os.path.exists(file_path):
        return file_path
    
    base_path, ext = os.path.splitext(file_path)
    counter = 1
    
    while True:
        new_file_path = f"{base_path}_{counter}{ext}"
        if not os.path.exists(new_file_path):
            return new_file_path
        counter += 1


def _load_template_from_cache(template_path, template_cache):
    """从缓存加载模板（进程内私有缓存）"""
    if template_path not in template_cache:
        with open(template_path, 'rb') as f:
            template_cache[template_path] = f.read()
    template_bytes = template_cache[template_path]
    return DocxTemplate(io.BytesIO(template_bytes))


def _worker_process_chunk(args):
    """Worker函数：处理一批文档（每个进程独立执行）
    
    Args:
        args: dict containing:
            - chunk_data: list of (index, row_dict) tuples
            - folder_field: 文件夹名字段
            - file_field: 文件名字段
            - ignore_missing: 是否忽略缺失占位符
            - output_path: 输出路径
            - word_template_path: 基础模板路径
            - template_mapping: 模板映射配置
            - chunk_start_idx: 这批数据的起始索引
            
    Returns:
        dict: success_count, error_messages, chunk_start_idx, elapsed_time
    """
    chunk_data = args['chunk_data']
    folder_field = args['folder_field']
    file_field = args['file_field']
    ignore_missing = args['ignore_missing']
    output_path = args['output_path']
    word_template_path = args['word_template_path']
    template_mapping = args['template_mapping']
    chunk_start_idx = args['chunk_start_idx']
    
    successful_count = 0
    error_messages = []
    start_time = time.time()
    
    # 验证模板文件
    valid_templates = {}
    template_paths_to_check = set([word_template_path])
    if template_mapping:
        for mapping in template_mapping.values():
            if isinstance(mapping, dict):
                template_paths_to_check.update(mapping.values())
    
    for tp in template_paths_to_check:
        if tp and os.path.exists(tp) and os.access(tp, os.R_OK):
            valid_templates[tp] = True
    
    # 进程内私有模板缓存
    template_cache = {}
    
    for idx, row_dict in chunk_data:
        try:
            folder_value = row_dict.get(folder_field, "") or row_dict.get(folder_field.replace(" ", "_"), "")
            file_value = row_dict.get(file_field, "") or row_dict.get(file_field.replace(" ", "_"), "")
            folder_name = _sanitize_filename(folder_value, f"文件夹_{idx}")
            file_name = _sanitize_filename(file_value, f"文件_{idx}")
            
            folder_path = os.path.join(output_path, folder_name)
            output_file = os.path.join(folder_path, f"{file_name}.docx")
            output_file = _unique_file_path(output_file)
            
            # 确定模板路径
            template_path = word_template_path
            if template_mapping:
                try:
                    if "__priority__" in template_mapping:
                        priority_fields = template_mapping["__priority__"]
                        for field in priority_fields:
                            if field in template_mapping:
                                mapping = template_mapping[field]
                                value = str(row_dict.get(field, "") or row_dict.get(field.replace(" ", "_"), "")).strip()
                                if value in mapping:
                                    template_path = mapping[value]
                                    break
                    else:
                        for field, mapping in template_mapping.items():
                            value = str(row_dict.get(field, "") or row_dict.get(field.replace(" ", "_"), "")).strip()
                            if value in mapping:
                                template_path = mapping[value]
                                break
                except Exception as e:
                    error_messages.append(f"第 {idx} 行: 选择模板失败: {str(e)}")
                    continue
            
            if not template_path or template_path not in valid_templates:
                if not template_path:
                    error_messages.append(f"第 {idx} 行: 未设置Word模板")
                else:
                    error_messages.append(f"第 {idx} 行: 模板文件不可用: {template_path}")
                continue
            
            # 加载并渲染模板
            try:
                doc = _load_template_from_cache(template_path, template_cache)
                
                context = {}
                first_row_keys = list(row_dict.keys())
                
                for col in first_row_keys:
                    try:
                        value = row_dict.get(col, "")
                        if isinstance(value, float) and value.is_integer():
                            value = int(value)
                        context[col] = str(value)
                    except Exception:
                        context[col] = ""
                
                # 处理"编号"字段
                number_field_found = False
                for col in list(context.keys()):
                    if col == "编号" or col == "编号".lower() or col == "编号".replace(" ", "_"):
                        if "编号" not in context:
                            context["编号"] = context[col]
                        number_field_found = True
                        break
                
                if not number_field_found:
                    for field in first_row_keys:
                        if "编号" in field or "num" in field.lower() or "id" in field.lower():
                            value = row_dict.get(field, "")
                            if isinstance(value, float) and value.is_integer():
                                value = int(value)
                            context["编号"] = str(value)
                            break
                
                if "编号" not in context:
                    context["编号"] = ""
                
                # 渲染文档
                try:
                    doc.render(context)
                except Exception as e:
                    if "not found in context" in str(e) or "context has no attribute" in str(e):
                        if ignore_missing:
                            doc.render(context, missing_tags='keep')
                        else:
                            error_messages.append(f"第 {idx} 行: 占位符缺失: {str(e)}")
                            continue
                    else:
                        raise
                
                doc.save(output_file)
                successful_count += 1
                
            except Exception as e:
                error_messages.append(f"第 {idx} 行: 生成文档失败: {str(e)}")
                
        except Exception as e:
            error_messages.append(f"第 {idx} 行: 处理失败: {str(e)}")
    
    elapsed_time = time.time() - start_time
    
    return {
        'success_count': successful_count,
        'error_messages': error_messages,
        'chunk_start_idx': chunk_start_idx,
        'elapsed_time': elapsed_time
    }


class DocumentGenerator:
    """文档生成器，支持单进程和多进程模式"""
    
    def __init__(self):
        pass
    
    def generate(self, rows, folder_field, file_field, ignore_missing, output_path, 
                 word_template_path, template_mapping, cancel_event, progress_cb,
                 use_multiprocessing=True, max_workers=None):
        """
        生成文档的核心方法
        
        Args:
            rows: Excel数据行的字典列表
            folder_field: 用于生成文件夹名的字段
            file_field: 用于生成文件名的字段
            ignore_missing: 是否忽略缺失的占位符
            output_path: 输出文件夹路径
            word_template_path: 基础Word模板路径
            template_mapping: 模板映射配置
            cancel_event: 取消事件
            progress_cb: 进度回调函数 (progress, message)
            use_multiprocessing: 是否使用多进程（默认True）
            max_workers: 最大进程数，默认CPU核心数
            
        Returns:
            tuple: (success_count, error_messages, exception)
        """
        logger.info("开始执行文档生成工作")
        start_time = time.time()
        
        try:
            total_rows = len(rows)
            
            if total_rows == 0:
                progress_cb(0, "没有要生成的数据")
                return 0, [], None
            
            # 检查Word模板是否存在
            if word_template_path and not os.path.exists(word_template_path):
                error_msg = f"基础Word模板不存在: {word_template_path}"
                logger.error(error_msg)
                return 0, [error_msg], None
            
            # 收集所有模板路径用于验证
            template_paths_to_check = set()
            template_paths_to_check.add(word_template_path)
            if template_mapping:
                for mapping in template_mapping.values():
                    if isinstance(mapping, dict):
                        template_paths_to_check.update(mapping.values())
            
            # 预检查所有模板文件
            valid_templates = {}
            for tp in template_paths_to_check:
                if tp and os.path.exists(tp) and os.access(tp, os.R_OK):
                    valid_templates[tp] = True
            
            if not valid_templates:
                error_msg = "没有可用的模板文件"
                logger.error(error_msg)
                return 0, [error_msg], None
            
            logger.info(f"已验证 {len(valid_templates)} 个模板文件")
            
            # 收集需要创建的文件夹
            folders_to_create = set()
            logger.info("预处理：收集需要创建的文件夹")
            for idx, row_dict in enumerate(rows, start=1):
                folder_value = row_dict.get(folder_field, "") or row_dict.get(folder_field.replace(" ", "_"), "")
                folder_name = _sanitize_filename(folder_value, f"文件夹_{idx}")
                folders_to_create.add(os.path.join(output_path, folder_name))
            
            # 批量创建文件夹
            logger.info(f"开始批量创建 {len(folders_to_create)} 个文件夹")
            for folder_path in folders_to_create:
                try:
                    os.makedirs(folder_path, exist_ok=True)
                except Exception as e:
                    logger.error(f"创建文件夹失败: {folder_path} - {str(e)}")
            
            # 如果不用多进程或数据量小，使用单进程模式
            if not use_multiprocessing or total_rows <= 50:
                successful_count, error_messages = self._generate_single_process(
                    rows, folder_field, file_field, ignore_missing, output_path,
                    word_template_path, template_mapping, valid_templates,
                    cancel_event, progress_cb, total_rows, start_time
                )
                return successful_count, error_messages, None
            
            # 多进程模式
            return self._generate_multiprocess(
                rows, folder_field, file_field, ignore_missing, output_path,
                word_template_path, template_mapping, valid_templates,
                cancel_event, progress_cb, total_rows, start_time, max_workers
            )
            
        except Exception as e:
            logger.critical(f"生成文档时发生致命错误: {str(e)}")
            return 0, [], e
    
    def _generate_single_process(self, rows, folder_field, file_field, ignore_missing, output_path,
                                  word_template_path, template_mapping, valid_templates,
                                  cancel_event, progress_cb, total_rows, start_time):
        """单进程生成"""
        successful_count = 0
        error_messages = []
        template_cache = {}
        
        for idx, row_dict in enumerate(rows, start=1):
            if cancel_event.is_set():
                logger.info(f"文档生成已被取消")
                break
            
            if idx % 10 == 0 or idx == total_rows:
                progress = int(idx / total_rows * 100)
                progress_cb(progress, f"正在生成第 {idx}/{total_rows} 个文档...")
            
            result = self._process_single_document(
                idx, row_dict, folder_field, file_field, ignore_missing, output_path,
                word_template_path, template_mapping, valid_templates, template_cache
            )
            
            if result['success']:
                successful_count += 1
            else:
                error_messages.extend(result['errors'])
        
        end_time = time.time()
        total_time = end_time - start_time
        logger.info(f"文档生成完成，成功 {successful_count} 个，失败 {len(error_messages)} 个，耗时 {total_time:.2f} 秒")
        progress_cb(100, f"文档生成完成，共耗时 {total_time:.2f} 秒")
        
        return successful_count, error_messages
    
    def _generate_multiprocess(self, rows, folder_field, file_field, ignore_missing, output_path,
                                word_template_path, template_mapping, valid_templates,
                                cancel_event, progress_cb, total_rows, start_time, max_workers):
        """多进程生成"""
        import multiprocessing
        
        # 计算进程数和每批大小
        if max_workers is None:
            max_workers = min(multiprocessing.cpu_count(), 8)  # 最多8个进程
        chunk_size = max(50, total_rows // max_workers)
        
        logger.info(f"使用 {max_workers} 个进程并行生成，每批 {chunk_size} 个文档")
        
        # 准备数据块
        chunks = []
        for i in range(0, total_rows, chunk_size):
            chunk_data = [(idx, rows[idx-1]) for idx in range(i+1, min(i+chunk_size, total_rows)+1)]
            chunks.append({
                'chunk_data': chunk_data,
                'folder_field': folder_field,
                'file_field': file_field,
                'ignore_missing': ignore_missing,
                'output_path': output_path,
                'word_template_path': word_template_path,
                'template_mapping': template_mapping,
                'chunk_start_idx': i + 1
            })
        
        # 使用进程池执行
        successful_count = 0
        error_messages = []
        completed = 0
        total_chunks = len(chunks)
        
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_worker_process_chunk, chunk): chunk for chunk in chunks}
            
            for future in as_completed(futures):
                if cancel_event.is_set():
                    # 取消所有待执行的任务
                    for f in futures:
                        f.cancel()
                    break
                
                completed += 1
                try:
                    result = future.result()
                    successful_count += result['success_count']
                    error_messages.extend(result['error_messages'])
                    
                    # 更新进度
                    progress = int(completed / total_chunks * 100)
                    elapsed = result.get('elapsed_time', 0)
                    progress_cb(progress, f"进程 {completed}/{total_chunks} 完成 (本批耗时 {elapsed:.1f}秒)")
                    
                except Exception as e:
                    logger.error(f"进程执行失败: {str(e)}")
                    error_messages.append(f"进程执行失败: {str(e)}")
        
        end_time = time.time()
        total_time = end_time - start_time
        logger.info(f"文档生成完成，成功 {successful_count} 个，失败 {len(error_messages)} 个，总耗时 {total_time:.2f} 秒")
        progress_cb(100, f"文档生成完成，共耗时 {total_time:.2f} 秒")

        return successful_count, error_messages, None
    
    def _process_single_document(self, idx, row_dict, folder_field, file_field, ignore_missing,
                                  output_path, word_template_path, template_mapping,
                                  valid_templates, template_cache):
        """处理单个文档"""
        try:
            folder_value = row_dict.get(folder_field, "") or row_dict.get(folder_field.replace(" ", "_"), "")
            file_value = row_dict.get(file_field, "") or row_dict.get(file_field.replace(" ", "_"), "")
            folder_name = _sanitize_filename(folder_value, f"文件夹_{idx}")
            file_name = _sanitize_filename(file_value, f"文件_{idx}")
            
            folder_path = os.path.join(output_path, folder_name)
            output_file = os.path.join(folder_path, f"{file_name}.docx")
            output_file = _unique_file_path(output_file)
            
            # 确定模板路径
            template_path = word_template_path
            if template_mapping:
                try:
                    if "__priority__" in template_mapping:
                        for field in template_mapping["__priority__"]:
                            if field in template_mapping:
                                mapping = template_mapping[field]
                                value = str(row_dict.get(field, "") or row_dict.get(field.replace(" ", "_"), "")).strip()
                                if value in mapping:
                                    template_path = mapping[value]
                                    break
                    else:
                        for field, mapping in template_mapping.items():
                            value = str(row_dict.get(field, "") or row_dict.get(field.replace(" ", "_"), "")).strip()
                            if value in mapping:
                                template_path = mapping[value]
                                break
                except Exception:
                    pass
            
            if not template_path or template_path not in valid_templates:
                return {'success': False, 'errors': [f"第 {idx} 行: 模板不可用"]}
            
            # 加载并渲染
            doc = _load_template_from_cache(template_path, template_cache)
            
            context = {}
            first_row_keys = list(row_dict.keys())
            
            for col in first_row_keys:
                try:
                    value = row_dict.get(col, "")
                    if isinstance(value, float) and value.is_integer():
                        value = int(value)
                    context[col] = str(value)
                except Exception:
                    context[col] = ""
            
            # 处理编号字段
            number_field_found = False
            for col in list(context.keys()):
                if col == "编号" or col == "编号".lower():
                    if "编号" not in context:
                        context["编号"] = context[col]
                    number_field_found = True
                    break
            
            if not number_field_found:
                for field in first_row_keys:
                    if "编号" in field or "num" in field.lower() or "id" in field.lower():
                        value = row_dict.get(field, "")
                        if isinstance(value, float) and value.is_integer():
                            value = int(value)
                        context["编号"] = str(value)
                        break
            
            if "编号" not in context:
                context["编号"] = ""
            
            # 渲染
            try:
                doc.render(context)
            except Exception as e:
                if "not found in context" in str(e) or "context has no attribute" in str(e):
                    if ignore_missing:
                        doc.render(context, missing_tags='keep')
                    else:
                        return {'success': False, 'errors': [f"第 {idx} 行: 占位符缺失"]}
                else:
                    raise
            
            doc.save(output_file)
            return {'success': True, 'errors': []}
            
        except Exception as e:
            return {'success': False, 'errors': [f"第 {idx} 行: {str(e)}"]}
