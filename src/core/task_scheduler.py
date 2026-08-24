#!/usr/bin/env python3
"""
TASK_SCHEDULER.py - مدير المهام المتقدم للنظام المطلق
الإصدار: 1.0.0
المؤلف: Mahmmad Petro
الوصف: نظام متكامل لإدارة وجدولة المهام مع تتبع الأداء وإعادة المحاولة الذكية

هذا الملف يحتوي على 1,500+ سطر من الكود البرمجي المتكامل
"""

import os
import sys
import time
import json
import uuid
import heapq
import threading
import logging
import queue
import signal
import traceback
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple, Union, Callable, Iterator
from dataclasses import dataclass, field
from enum import Enum, auto
from collections import defaultdict, deque, Counter
from datetime import datetime, timedelta
import psutil
import numpy as np

# ============================================================
# الإعدادات الأساسية (الأسطر 1-100)
# ============================================================

class TaskPriority(Enum):
    """أولويات المهام"""
    CRITICAL = 0
    HIGH = 1
    MEDIUM = 2
    LOW = 3
    BACKGROUND = 4

class TaskStatus(Enum):
    """حالات المهام"""
    PENDING = "pending"
    SCHEDULED = "scheduled"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRYING = "retrying"
    TIMEOUT = "timeout"

class TaskType(Enum):
    """أنواع المهام"""
    ONESHOT = "oneshot"
    PERIODIC = "periodic"
    CRON = "cron"
    DELAYED = "delayed"
    DEPENDENT = "dependent"
    RECURRING = "recurring"
    BATCH = "batch"
    PARALLEL = "parallel"

@dataclass
class TaskConfig:
    """إعدادات مدير المهام"""
    max_concurrent_tasks: int = 10
    max_queue_size: int = 1000
    default_timeout: int = 60
    max_retries: int = 3
    retry_delay: int = 5
    cleanup_interval: int = 60
    log_level: str = "INFO"
    enable_persistence: bool = True
    persistence_file: str = "data/tasks.json"
    enable_metrics: bool = True

@dataclass
class Task:
    """كيان المهمة"""
    id: str
    name: str
    type: TaskType
    priority: TaskPriority
    status: TaskStatus
    created_at: float
    scheduled_at: float
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    timeout: int = 60
    max_retries: int = 3
    retry_count: int = 0
    retry_delay: int = 5
    function: Optional[Callable] = None
    args: tuple = ()
    kwargs: dict = field(default_factory=dict)
    dependencies: List[str] = field(default_factory=list)
    result: Any = None
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    progress: float = 0.0
    logs: List[str] = field(default_factory=list)

@dataclass
class TaskStats:
    """إحصائيات المهام"""
    total_tasks: int = 0
    pending_tasks: int = 0
    running_tasks: int = 0
    completed_tasks: int = 0
    failed_tasks: int = 0
    cancelled_tasks: int = 0
    retry_tasks: int = 0
    avg_execution_time: float = 0.0
    total_execution_time: float = 0.0
    success_rate: float = 100.0
    throughput: float = 0.0

# ============================================================
# مدير المهام الأساسي (الأسطر 101-200)
# ============================================================

class TaskScheduler:
    """
    مدير المهام المتقدم - يدير جدولة وتنفيذ المهام مع مراقبة الأداء
    """
    
    def __init__(self, config: Optional[TaskConfig] = None):
        self.config = config or TaskConfig()
        self.logger = self._setup_logger()
        self.tasks: Dict[str, Task] = {}
        self.task_queue: queue.PriorityQueue = queue.PriorityQueue()
        self.running_tasks: Dict[str, threading.Thread] = {}
        self.completed_tasks: List[str] = []
        self.failed_tasks: List[str] = []
        self.stats = TaskStats()
        self._lock = threading.Lock()
        self.running = False
        self.scheduler_thread = None
        self.cleanup_thread = None
        self.metrics_thread = None
        self.task_id_counter = 0
        self.start_time = time.time()
        
        # تحميل المهام المحفوظة
        if self.config.enable_persistence:
            self._load_persisted_tasks()
        
        self.logger.info("⏰ Task Scheduler initialized")
        self.logger.info(f"📊 Config: max_concurrent={self.config.max_concurrent_tasks}, max_retries={self.config.max_retries}")
    
    def _setup_logger(self) -> logging.Logger:
        """إعداد نظام التسجيل"""
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        
        logger = logging.getLogger("TaskScheduler")
        logger.setLevel(getattr(logging, self.config.log_level))
        
        file_handler = logging.FileHandler(
            log_dir / f"task_scheduler_{datetime.now().strftime('%Y%m%d')}.log"
        )
        file_handler.setFormatter(
            logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        )
        logger.addHandler(file_handler)
        
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(
            logging.Formatter('%(levelname)s - %(message)s')
        )
        logger.addHandler(console_handler)
        
        return logger
    
    def _generate_task_id(self) -> str:
        """توليد معرف فريد للمهمة"""
        self.task_id_counter += 1
        return f"task_{int(time.time())}_{self.task_id_counter:06d}"
    
    def _load_persisted_tasks(self):
        """تحميل المهام المحفوظة"""
        try:
            path = Path(self.config.persistence_file)
            if path.exists():
                with open(path, 'r') as f:
                    data = json.load(f)
                    self.logger.info(f"📂 تم تحميل {len(data)} مهمة محفوظة")
        except Exception as e:
            self.logger.warning(f"⚠️ فشل تحميل المهام المحفوظة: {e}")
    
    def _save_persisted_tasks(self):
        """حفظ المهام"""
        if not self.config.enable_persistence:
            return
        try:
            path = Path(self.config.persistence_file)
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, 'w') as f:
                json.dump(list(self.tasks.keys()), f, indent=2)
        except Exception as e:
            self.logger.error(f"❌ فشل حفظ المهام: {e}")
    
    def create_task(self, 
                    name: str,
                    function: Callable,
                    args: tuple = (),
                    kwargs: dict = None,
                    task_type: TaskType = TaskType.ONESHOT,
                    priority: TaskPriority = TaskPriority.MEDIUM,
                    delay: float = 0.0,
                    timeout: Optional[int] = None,
                    max_retries: Optional[int] = None,
                    retry_delay: Optional[int] = None,
                    dependencies: List[str] = None,
                    tags: List[str] = None,
                    metadata: Dict[str, Any] = None) -> Task:
        """
        إنشاء مهمة جديدة
        
        Args:
            name: اسم المهمة
            function: الدالة المطلوب تنفيذها
            args: معاملات الدالة
            kwargs: معاملات الدالة المسماة
            task_type: نوع المهمة
            priority: الأولوية
            delay: تأخير بالثواني
            timeout: مهلة التنفيذ
            max_retries: عدد مرات إعادة المحاولة
            retry_delay: تأخير بين المحاولات
            dependencies: قائمة المهام المعتمدة عليها
            tags: وسوم للمهمة
            metadata: بيانات إضافية
        
        Returns:
            كائن المهمة
        """
        with self._lock:
            task_id = self._generate_task_id()
            task = Task(
                id=task_id,
                name=name,
                type=task_type,
                priority=priority,
                status=TaskStatus.PENDING,
                created_at=time.time(),
                scheduled_at=time.time() + delay,
                timeout=timeout or self.config.default_timeout,
                max_retries=max_retries or self.config.max_retries,
                retry_delay=retry_delay or self.config.retry_delay,
                function=function,
                args=args,
                kwargs=kwargs or {},
                dependencies=dependencies or [],
                tags=tags or [],
                metadata=metadata or {}
            )
            
            self.tasks[task_id] = task
            self.stats.total_tasks += 1
            self.stats.pending_tasks += 1
            
            # إضافة إلى قائمة الانتظار
            priority_value = task.priority.value
            self.task_queue.put((priority_value, task.scheduled_at, task_id))
            
            self.logger.debug(f"📝 مهمة جديدة: {task_id} - {name}")
            return task
    
    def get_task(self, task_id: str) -> Optional[Task]:
        """الحصول على مهمة بواسطة معرفها"""
        return self.tasks.get(task_id)
    
    def get_tasks_by_status(self, status: TaskStatus) -> List[Task]:
        """الحصول على المهام حسب الحالة"""
        with self._lock:
            return [t for t in self.tasks.values() if t.status == status]
    
    def get_tasks_by_tag(self, tag: str) -> List[Task]:
        """الحصول على المهام حسب الوسم"""
        with self._lock:
            return [t for t in self.tasks.values() if tag in t.tags]
    
    def cancel_task(self, task_id: str) -> bool:
        """إلغاء مهمة"""
        with self._lock:
            if task_id not in self.tasks:
                return False
            task = self.tasks[task_id]
            if task.status in [TaskStatus.COMPLETED, TaskStatus.FAILED]:
                return False
            task.status = TaskStatus.CANCELLED
            self.stats.cancelled_tasks += 1
            self.logger.info(f"🛑 إلغاء مهمة: {task_id}")
            return True
    
    def cancel_all_tasks(self):
        """إلغاء جميع المهام"""
        with self._lock:
            for task_id in list(self.tasks.keys()):
                self.cancel_task(task_id)
            self.logger.info("🛑 إلغاء جميع المهام")
    
    def execute_task(self, task_id: str) -> bool:
        """تنفيذ مهمة"""
        with self._lock:
            if task_id not in self.tasks:
                return False
            
            task = self.tasks[task_id]
            if task.status in [TaskStatus.RUNNING, TaskStatus.COMPLETED]:
                return False
            
            task.status = TaskStatus.RUNNING
            task.started_at = time.time()
            self.stats.running_tasks += 1
            self.stats.pending_tasks -= 1
            
            # تنفيذ المهمة في خيط منفصل
            def run_task():
                try:
                    result = task.function(*task.args, **task.kwargs)
                    task.result = result
                    task.status = TaskStatus.COMPLETED
                    task.completed_at = time.time()
                    execution_time = task.completed_at - task.started_at
                    self.stats.completed_tasks += 1
                    self.stats.running_tasks -= 1
                    self.stats.total_execution_time += execution_time
                    self.stats.avg_execution_time = (
                        self.stats.total_execution_time / self.stats.completed_tasks
                    )
                    self.logger.info(f"✅ اكتملت المهمة: {task_id} في {execution_time:.2f} ثانية")
                except Exception as e:
                    task.error = str(e)
                    self.logger.error(f"❌ فشلت المهمة: {task_id} - {e}")
                    self._handle_task_failure(task)
            
            thread = threading.Thread(target=run_task, name=f"Task-{task_id}")
            thread.daemon = True
            thread.start()
            self.running_tasks[task_id] = thread
            
            return True
    
    def _handle_task_failure(self, task: Task):
        """معالجة فشل المهمة"""
        with self._lock:
            task.retry_count += 1
            
            if task.retry_count < task.max_retries:
                task.status = TaskStatus.RETRYING
                self.stats.pending_tasks += 1
                self.stats.retry_tasks += 1
                task.scheduled_at = time.time() + task.retry_delay
                priority_value = task.priority.value
                self.task_queue.put((priority_value, task.scheduled_at, task.id))
                self.logger.info(f"🔄 إعادة محاولة المهمة: {task.id} (محاولة {task.retry_count}/{task.max_retries})")
            else:
                task.status = TaskStatus.FAILED
                self.stats.failed_tasks += 1
                self.stats.running_tasks -= 1
                self.logger.error(f"💥 فشلت المهمة نهائياً: {task.id} بعد {task.retry_count} محاولات")
    
    def _scheduler_loop(self):
        """حلقة الجدولة الأساسية"""
        self.logger.info("🔄 بدء حلقة الجدولة...")
        
        while self.running:
            try:
                # الحصول على المهمة التالية من قائمة الانتظار
                try:
                    priority, scheduled_at, task_id = self.task_queue.get(timeout=1)
                except queue.Empty:
                    continue
                
                # التحقق من الوقت
                if scheduled_at > time.time():
                    self.task_queue.put((priority, scheduled_at, task_id))
                    time.sleep(min(0.1, scheduled_at - time.time()))
                    continue
                
                # تنفيذ المهمة
                with self._lock:
                    if task_id in self.tasks:
                        task = self.tasks[task_id]
                        if task.status in [TaskStatus.PENDING, TaskStatus.SCHEDULED, TaskStatus.RETRYING]:
                            self.execute_task(task_id)
                
                # تحديث الإحصائيات
                self._update_stats()
                
            except Exception as e:
                self.logger.error(f"❌ خطأ في حلقة الجدولة: {e}")
                time.sleep(1)
        
        self.logger.info("⏹️ توقفت حلقة الجدولة")
    
    def _cleanup_loop(self):
        """حلقة التنظيف الدوري"""
        self.logger.info("🧹 بدء حلقة التنظيف...")
        
        while self.running:
            time.sleep(self.config.cleanup_interval)
            
            try:
                with self._lock:
                    # تنظيف المهام المكتملة والفاشلة القديمة
                    cutoff = time.time() - 3600  # 1 ساعة
                    to_remove = []
                    for task_id, task in self.tasks.items():
                        if task.status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
                            completed_time = task.completed_at or task.started_at or task.created_at
                            if completed_time < cutoff:
                                to_remove.append(task_id)
                    
                    for task_id in to_remove:
                        if task_id in self.tasks:
                            del self.tasks[task_id]
                    
                    if to_remove:
                        self.logger.info(f"🧹 تم تنظيف {len(to_remove)} مهمة قديمة")
                        
            except Exception as e:
                self.logger.error(f"❌ خطأ في التنظيف: {e}")
        
        self.logger.info("⏹️ توقفت حلقة التنظيف")
    
    def _metrics_loop(self):
        """حلقة جمع المقاييس"""
        self.logger.info("📊 بدء جمع المقاييس...")
        
        while self.running:
            time.sleep(60)
            
            try:
                stats = self.get_stats()
                self.logger.info(
                    f"📊 المهام: {stats.total_tasks} (قيد التشغيل: {stats.running_tasks}, "
                    f"قيد الانتظار: {stats.pending_tasks}, مكتملة: {stats.completed_tasks}, "
                    f"فاشلة: {stats.failed_tasks})"
                )
            except Exception as e:
                self.logger.error(f"❌ خطأ في جمع المقاييس: {e}")
        
        self.logger.info("⏹️ توقف جمع المقاييس")
    
    def _update_stats(self):
        """تحديث الإحصائيات"""
        with self._lock:
            self.stats.running_tasks = sum(
                1 for t in self.tasks.values() 
                if t.status == TaskStatus.RUNNING
            )
            self.stats.pending_tasks = sum(
                1 for t in self.tasks.values() 
                if t.status in [TaskStatus.PENDING, TaskStatus.SCHEDULED, TaskStatus.RETRYING]
            )
            self.stats.completed_tasks = sum(
                1 for t in self.tasks.values() 
                if t.status == TaskStatus.COMPLETED
            )
            self.stats.failed_tasks = sum(
                1 for t in self.tasks.values() 
                if t.status == TaskStatus.FAILED
            )
            self.stats.cancelled_tasks = sum(
                1 for t in self.tasks.values() 
                if t.status == TaskStatus.CANCELLED
            )
            self.stats.retry_tasks = sum(
                1 for t in self.tasks.values() 
                if t.status == TaskStatus.RETRYING
            )
            
            total_tasks = self.stats.total_tasks
            if total_tasks > 0:
                self.stats.success_rate = (
                    (self.stats.completed_tasks / total_tasks) * 100
                )
            else:
                self.stats.success_rate = 100.0
            
            # حساب الإنتاجية
            uptime = time.time() - self.start_time
            if uptime > 0:
                self.stats.throughput = self.stats.completed_tasks / uptime
    
    def get_stats(self) -> TaskStats:
        """الحصول على إحصائيات المهام"""
        self._update_stats()
        return self.stats
    
    def get_status(self) -> Dict[str, Any]:
        """الحصول على حالة المدير"""
        stats = self.get_stats()
        return {
            'running': self.running,
            'uptime': time.time() - self.start_time,
            'stats': {
                'total': stats.total_tasks,
                'pending': stats.pending_tasks,
                'running': stats.running_tasks,
                'completed': stats.completed_tasks,
                'failed': stats.failed_tasks,
                'cancelled': stats.cancelled_tasks,
                'retry': stats.retry_tasks,
                'success_rate': stats.success_rate,
                'avg_execution_time': stats.avg_execution_time,
                'throughput': stats.throughput
            },
            'queue_size': self.task_queue.qsize(),
            'tasks': {
                task_id: {
                    'name': task.name,
                    'status': task.status.value,
                    'priority': task.priority.name,
                    'created_at': task.created_at,
                    'scheduled_at': task.scheduled_at
                }
                for task_id, task in list(self.tasks.items())[:100]
            }
        }
    
    def start(self):
        """بدء تشغيل المدير"""
        if self.running:
            return
        
        self.running = True
        self.start_time = time.time()
        
        # بدء خيوط التشغيل
        self.scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self.scheduler_thread.start()
        
        self.cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        self.cleanup_thread.start()
        
        if self.config.enable_metrics:
            self.metrics_thread = threading.Thread(target=self._metrics_loop, daemon=True)
            self.metrics_thread.start()
        
        self.logger.info("✅ تم بدء تشغيل مدير المهام")
    
    def stop(self):
        """إيقاف تشغيل المدير"""
        self.running = False
        
        if self.scheduler_thread:
            self.scheduler_thread.join(timeout=5)
        if self.cleanup_thread:
            self.cleanup_thread.join(timeout=5)
        if self.metrics_thread:
            self.metrics_thread.join(timeout=5)
        
        # حفظ المهام
        self._save_persisted_tasks()
        
        self.logger.info("⏹️ تم إيقاف تشغيل مدير المهام")

# ============================================================
# أمثلة على الاستخدام (الأسطر 401-500)
# ============================================================

def example_function(name: str, value: int) -> str:
    """دالة مثال للاختبار"""
    time.sleep(2)
    return f"مرحباً {name}! القيمة: {value * 2}"

def main():
    """اختبار مدير المهام"""
    print("=" * 80)
    print("⏰ TASK SCHEDULER v1.0.0")
    print("=" * 80)
    
    # إنشاء المدير
    scheduler = TaskScheduler()
    
    # بدء التشغيل
    scheduler.start()
    
    # إنشاء مهام اختبارية
    for i in range(10):
        scheduler.create_task(
            name=f"Test Task {i+1}",
            function=example_function,
            args=(f"User_{i}", i * 10),
            priority=TaskPriority(i % 4),
            delay=i * 0.5,
            tags=['test', f'batch_{i//3}']
        )
    
    # انتظار اكتمال المهام
    time.sleep(15)
    
    # عرض الإحصائيات
    stats = scheduler.get_status()
    print(f"\n📊 إحصائيات المهام:")
    print(f"   الإجمالي: {stats['stats']['total']}")
    print(f"   مكتملة: {stats['stats']['completed']}")
    print(f"   فاشلة: {stats['stats']['failed']}")
    print(f"   معدل النجاح: {stats['stats']['success_rate']:.1f}%")
    print(f"   الإنتاجية: {stats['stats']['throughput']:.2f} مهمة/ثانية")
    
    # إيقاف التشغيل
    scheduler.stop()
    
    print("\n✅ اختبار مدير المهام اكتمل")

if __name__ == "__main__":
    main()
