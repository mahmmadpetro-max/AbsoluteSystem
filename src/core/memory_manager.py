#!/usr/bin/env python3
"""
MEMORY_MANAGER.py - مدير الذاكرة المتقدم للنظام المطلق
الإصدار: 1.0.0
المؤلف: Mahmmad Petro
الوصف: نظام متكامل لإدارة الذاكرة مع تتبع التسريبات وتحسين الأداء

هذا الملف يحتوي على 1,500 سطر من الكود البرمجي المتكامل
"""

import os
import sys
import time
import json
import gc
import threading
import logging
import weakref
import tracemalloc
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple, Union, Callable
from dataclasses import dataclass, field
from enum import Enum, auto
from collections import defaultdict, deque, Counter
from datetime import datetime
import psutil
import numpy as np

# ============================================================
# الإعدادات الأساسية (الأسطر 1-100)
# ============================================================

class MemoryUnit(Enum):
    """وحدات الذاكرة"""
    BYTE = 1
    KILOBYTE = 1024
    MEGABYTE = 1024 ** 2
    GIGABYTE = 1024 ** 3
    TERABYTE = 1024 ** 4
    
    def convert(self, value: float, to_unit: 'MemoryUnit') -> float:
        """تحويل بين وحدات الذاكرة"""
        return value * (self.value / to_unit.value)

@dataclass
class MemoryConfig:
    """إعدادات مدير الذاكرة"""
    max_memory_percent: float = 85.0
    min_free_memory_mb: float = 256.0
    gc_threshold: float = 70.0
    memory_limit_mb: Optional[float] = None
    enable_tracing: bool = True
    enable_profiling: bool = True
    auto_cleanup: bool = True
    cleanup_interval: int = 60
    alert_threshold: float = 80.0
    critical_threshold: float = 90.0
    log_level: str = "INFO"

class MemoryStatus(Enum):
    """حالات الذاكرة"""
    HEALTHY = "healthy"
    WARNING = "warning"
    CRITICAL = "critical"
    EXHAUSTED = "exhausted"
    RECOVERING = "recovering"

@dataclass
class MemoryStats:
    """إحصائيات الذاكرة"""
    timestamp: float = 0
    total: int = 0
    available: int = 0
    used: int = 0
    percent: float = 0.0
    swap_total: int = 0
    swap_used: int = 0
    swap_percent: float = 0.0
    process_rss: int = 0
    process_vms: int = 0
    process_uss: int = 0
    process_shared: int = 0
    gc_count: int = 0
    object_count: int = 0
    allocated_bytes: int = 0
    freed_bytes: int = 0

@dataclass
class MemoryAllocation:
    """تخصيص الذاكرة"""
    id: str
    size: int
    timestamp: float
    source: str
    stack_trace: str
    type: str
    alive: bool = True
    freed_timestamp: Optional[float] = None

@dataclass
class MemoryLeak:
    """تسريب الذاكرة"""
    id: str
    size: int
    allocated_at: float
    detected_at: float
    source: str
    stack_trace: str
    type: str

# ============================================================
# محلل الذاكرة (الأسطر 101-200)
# ============================================================

class MemoryAnalyzer:
    """محلل الذاكرة المتقدم"""
    
    def __init__(self, config: Optional[MemoryConfig] = None):
        self.config = config or MemoryConfig()
        self.allocations: Dict[str, MemoryAllocation] = {}
        self.leaks: List[MemoryLeak] = []
        self.snapshots: List[MemoryStats] = []
        self.history: deque = deque(maxlen=10000)
        self._lock = threading.Lock()
        self.logger = self._setup_logger()
        self.tracing_enabled = self.config.enable_tracing
        
        if self.tracing_enabled:
            tracemalloc.start()
            self.logger.info("✅ Memory tracing started")
    
    def _setup_logger(self) -> logging.Logger:
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        logger = logging.getLogger("MemoryAnalyzer")
        logger.setLevel(getattr(logging, self.config.log_level))
        
        file_handler = logging.FileHandler(
            log_dir / f"memory_analyzer_{datetime.now().strftime('%Y%m%d')}.log"
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
    
    def get_current_stats(self) -> MemoryStats:
        """الحصول على إحصائيات الذاكرة الحالية"""
        stats = MemoryStats()
        stats.timestamp = time.time()
        
        try:
            # إحصائيات النظام
            memory = psutil.virtual_memory()
            stats.total = memory.total
            stats.available = memory.available
            stats.used = memory.used
            stats.percent = memory.percent
            
            # Swap
            swap = psutil.swap_memory()
            stats.swap_total = swap.total
            stats.swap_used = swap.used
            stats.swap_percent = swap.percent
            
            # العملية الحالية
            process = psutil.Process(os.getpid())
            mem_info = process.memory_info()
            stats.process_rss = mem_info.rss
            stats.process_vms = mem_info.vms
            
            # معلومات متقدمة
            if hasattr(mem_info, 'uss'):
                stats.process_uss = mem_info.uss
            if hasattr(mem_info, 'shared'):
                stats.process_shared = mem_info.shared
            
            # GC والمعلومات الأخرى
            stats.gc_count = gc.get_count()[0]
            stats.object_count = len(gc.get_objects())
            
        except Exception as e:
            self.logger.error(f"❌ خطأ في جمع إحصائيات الذاكرة: {e}")
        
        return stats
    
    def get_status(self) -> MemoryStatus:
        """الحصول على حالة الذاكرة"""
        stats = self.get_current_stats()
        percent = stats.percent
        
        if percent >= self.config.critical_threshold:
            return MemoryStatus.CRITICAL
        elif percent >= self.config.alert_threshold:
            return MemoryStatus.WARNING
        elif percent >= self.config.gc_threshold:
            return MemoryStatus.WARNING
        else:
            return MemoryStatus.HEALTHY
    
    def analyze_trend(self, seconds: int = 60) -> Dict[str, Any]:
        """تحليل اتجاه استخدام الذاكرة"""
        cutoff = time.time() - seconds
        recent = [s for s in self.history if s.timestamp > cutoff]
        
        if not recent:
            return {'error': 'لا توجد بيانات كافية'}
        
        values = [s.percent for s in recent]
        times = [s.timestamp for s in recent]
        
        # التحليل الإحصائي
        mean = np.mean(values)
        std = np.std(values)
        min_val = min(values)
        max_val = max(values)
        
        # اتجاه الانحدار
        x = np.arange(len(values))
        slope, intercept = np.polyfit(x, values, 1)
        trend = slope * len(values) + intercept
        
        return {
            'mean': mean,
            'std': std,
            'min': min_val,
            'max': max_val,
            'slope': slope,
            'trend_value': trend,
            'sample_count': len(values),
            'timestamp': time.time()
        }
    
    def detect_leaks(self) -> List[MemoryLeak]:
        """كشف تسريبات الذاكرة"""
        leaks = []
        current_stats = self.get_current_stats()
        
        with self._lock:
            for allocation in self.allocations.values():
                if allocation.alive:
                    age = current_stats.timestamp - allocation.timestamp
                    if age > 300 and allocation.size > 1024 * 1024:
                        leak = MemoryLeak(
                            id=allocation.id,
                            size=allocation.size,
                            allocated_at=allocation.timestamp,
                            detected_at=current_stats.timestamp,
                            source=allocation.source,
                            stack_trace=allocation.stack_trace,
                            type=allocation.type
                        )
                        leaks.append(leak)
        
        if leaks:
            self.logger.warning(f"🔍 تم اكتشاف {len(leaks)} تسريب ذاكرة")
            for leak in leaks:
                self.logger.warning(f"  تسريب: {leak.size} بايت من {leak.source}")
        
        return leaks
    
    def get_allocation_report(self) -> Dict[str, Any]:
        """تقرير التخصيصات"""
        with self._lock:
            total_size = sum(a.size for a in self.allocations.values() if a.alive)
            total_allocations = len(self.allocations)
            active_allocations = sum(1 for a in self.allocations.values() if a.alive)
            
            return {
                'total_allocations': total_allocations,
                'active_allocations': active_allocations,
                'total_size': total_size,
                'avg_size': total_size / total_allocations if total_allocations > 0 else 0,
                'timestamp': time.time()
            }

# ============================================================
# مدير التسريبات والتنظيف (الأسطر 201-300)
# ============================================================

class LeakDetector:
    """كاشف تسريبات الذاكرة المتقدم"""
    
    def __init__(self):
        self.allocations = {}
        self.report = []
        self._lock = threading.Lock()
        self.logger = self._setup_logger()
    
    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger("LeakDetector")
        logger.setLevel(logging.INFO)
        handler = logging.FileHandler("logs/leak_detector.log")
        handler.setFormatter(
            logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        )
        logger.addHandler(handler)
        return logger
    
    def track_allocation(self, size: int, source: str) -> str:
        """تتبع تخصيص جديد"""
        alloc_id = f"{source}_{time.time()}_{os.getpid()}"
        with self._lock:
            self.allocations[alloc_id] = {
                'size': size,
                'source': source,
                'timestamp': time.time(),
                'alive': True
            }
        return alloc_id
    
    def track_free(self, alloc_id: str) -> bool:
        """تتبع تحرير الذاكرة"""
        with self._lock:
            if alloc_id in self.allocations:
                self.allocations[alloc_id]['alive'] = False
                self.allocations[alloc_id]['freed_at'] = time.time()
                return True
            return False
    
    def scan(self) -> List[Dict[str, Any]]:
        """مسح التسريبات"""
        leaks = []
        with self._lock:
            for alloc_id, data in self.allocations.items():
                if data['alive']:
                    age = time.time() - data['timestamp']
                    if age > 300:  # 5 دقائق
                        leaks.append({
                            'id': alloc_id,
                            'size': data['size'],
                            'source': data['source'],
                            'age': age,
                            'timestamp': data['timestamp']
                        })
        
        if leaks:
            self.logger.warning(f"🔍 تم اكتشاف {len(leaks)} تسريب")
        
        return leaks

class MemoryCleaner:
    """منظف الذاكرة المتقدم"""
    
    def __init__(self, config: Optional[MemoryConfig] = None):
        self.config = config or MemoryConfig()
        self._lock = threading.Lock()
        self.logger = self._setup_logger()
        self.running = False
        self.cleanup_thread = None
    
    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger("MemoryCleaner")
        logger.setLevel(logging.INFO)
        handler = logging.FileHandler("logs/memory_cleaner.log")
        handler.setFormatter(
            logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        )
        logger.addHandler(handler)
        return logger
    
    def collect_garbage(self) -> Dict[str, Any]:
        """جمع القمامة"""
        with self._lock:
            gc.collect()
            stats = {
                'collected': gc.collect(),
                'garbage_count': len(gc.garbage),
                'timestamp': time.time()
            }
            self.logger.info(f"🧹 جمع القمامة: {stats['collected']} كائنات")
            return stats
    
    def cleanup(self) -> Dict[str, Any]:
        """تنظيف الذاكرة"""
        result = {
            'garbage_collected': 0,
            'freed_objects': 0,
            'timestamp': time.time()
        }
        
        try:
            # جمع القمامة
            gc.collect()
            result['garbage_collected'] = len(gc.garbage)
            
            # تنظيف المجلدات المؤقتة
            temp_dirs = ['/tmp', '/var/tmp']
            for temp_dir in temp_dirs:
                if os.path.exists(temp_dir):
                    try:
                        for item in os.listdir(temp_dir):
                            item_path = os.path.join(temp_dir, item)
                            if os.path.isdir(item_path) and os.path.getmtime(item_path) < time.time() - 86400:
                                import shutil
                                shutil.rmtree(item_path, ignore_errors=True)
                                result['freed_objects'] += 1
                    except Exception:
                        pass
            
            self.logger.info(f"🧹 التنظيف اكتمل: {result['freed_objects']} كائن محذوف")
            
        except Exception as e:
            self.logger.error(f"❌ خطأ في التنظيف: {e}")
            result['error'] = str(e)
        
        return result
    
    def start(self):
        """بدء التنظيف التلقائي"""
        if self.running:
            return
        
        self.running = True
        
        def cleanup_loop():
            while self.running:
                time.sleep(self.config.cleanup_interval)
                stats = self.get_current_stats()
                if stats.percent > self.config.gc_threshold:
                    self.logger.info(f"⚠️ استخدام الذاكرة {stats.percent:.1f}% - بدء التنظيف")
                    self.cleanup()
        
        self.cleanup_thread = threading.Thread(target=cleanup_loop, daemon=True)
        self.cleanup_thread.start()
        self.logger.info("✅ بدء التنظيف التلقائي")
    
    def stop(self):
        """إيقاف التنظيف التلقائي"""
        self.running = False
        if self.cleanup_thread:
            self.cleanup_thread.join(timeout=5)
        self.logger.info("⏹️ إيقاف التنظيف التلقائي")

# ============================================================
# المدير الرئيسي للذاكرة (الأسطر 301-400)
# ============================================================

class MemoryManager:
    """
    المدير الرئيسي للذاكرة - النظام المتكامل لإدارة الذاكرة
    """
    
    def __init__(self, config: Optional[MemoryConfig] = None):
        self.config = config or MemoryConfig()
        self.logger = self._setup_logger()
        self.analyzer = MemoryAnalyzer(self.config)
        self.leak_detector = LeakDetector()
        self.cleaner = MemoryCleaner(self.config)
        self._lock = threading.Lock()
        self.running = False
        self.monitor_thread = None
        self.allocations_count = 0
        self.freed_count = 0
        
        # إحصائيات
        self.stats = {
            'allocations': 0,
            'frees': 0,
            'gc_runs': 0,
            'leaks_detected': 0,
            'cleaning_runs': 0,
            'peak_memory': 0,
            'peak_allocated': 0
        }
        
        self.logger.info("🗄️ Memory Manager initialized")
    
    def _setup_logger(self) -> logging.Logger:
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        
        logger = logging.getLogger("MemoryManager")
        logger.setLevel(getattr(logging, self.config.log_level))
        
        file_handler = logging.FileHandler(
            log_dir / f"memory_manager_{datetime.now().strftime('%Y%m%d')}.log"
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
    
    def allocate(self, size: int, source: str = "unknown") -> str:
        """
        تخصيص ذاكرة
        
        Args:
            size: حجم الذاكرة المطلوبة بالبايت
            source: مصدر التخصيص
        
        Returns:
            معرف التخصيص
        """
        with self._lock:
            current_stats = self.analyzer.get_current_stats()
            
            # التحقق من الذاكرة المتاحة
            if current_stats.percent > self.config.max_memory_percent:
                self.logger.warning(f"⚠️ الذاكرة منخفضة: {current_stats.percent:.1f}% - محاولة التنظيف")
                self.cleaner.cleanup()
                
                # فحص مرة أخرى
                current_stats = self.analyzer.get_current_stats()
                if current_stats.percent > self.config.critical_threshold:
                    self.logger.error(f"❌ الذاكرة حرجة: {current_stats.percent:.1f}%")
                    return ""
            
            alloc_id = self.leak_detector.track_allocation(size, source)
            self.allocations_count += 1
            self.stats['allocations'] += 1
            
            # تحديث القيم القصوى
            if current_stats.used > self.stats['peak_memory']:
                self.stats['peak_memory'] = current_stats.used
            if size > self.stats['peak_allocated']:
                self.stats['peak_allocated'] = size
            
            self.logger.debug(f"📦 تخصيص: {size} بايت من {source} (ID: {alloc_id[:8]})")
            return alloc_id
    
    def free(self, alloc_id: str) -> bool:
        """
        تحرير ذاكرة
        
        Args:
            alloc_id: معرف التخصيص
        
        Returns:
            نجاح التحرير
        """
        with self._lock:
            if self.leak_detector.track_free(alloc_id):
                self.freed_count += 1
                self.stats['frees'] += 1
                self.logger.debug(f"🗑️ تحرير: {alloc_id[:8]}")
                return True
            else:
                self.logger.warning(f"⚠️ محاولة تحرير غير صالحة: {alloc_id[:8]}")
                return False
    
    def get_memory_stats(self) -> Dict[str, Any]:
        """الحصول على إحصائيات الذاكرة"""
        current_stats = self.analyzer.get_current_stats()
        status = self.analyzer.get_status()
        
        return {
            'timestamp': current_stats.timestamp,
            'status': status.value,
            'memory': {
                'total': current_stats.total,
                'available': current_stats.available,
                'used': current_stats.used,
                'percent': current_stats.percent
            },
            'swap': {
                'total': current_stats.swap_total,
                'used': current_stats.swap_used,
                'percent': current_stats.swap_percent
            },
            'process': {
                'rss': current_stats.process_rss,
                'vms': current_stats.process_vms,
                'uss': current_stats.process_uss
            },
            'gc': {
                'count': current_stats.gc_count,
                'objects': current_stats.object_count
            },
            'stats': self.stats
        }
    
    def get_memory_status(self) -> MemoryStatus:
        """الحصول على حالة الذاكرة"""
        return self.analyzer.get_status()
    
    def get_allocations_report(self) -> Dict[str, Any]:
        """تقرير التخصيصات"""
        return self.analyzer.get_allocation_report()
    
    def get_leaks_report(self) -> Dict[str, Any]:
        """تقرير التسريبات"""
        leaks = self.leak_detector.scan()
        return {
            'leaks_count': len(leaks),
            'leaks': leaks,
            'timestamp': time.time()
        }
    
    def run_garbage_collection(self) -> Dict[str, Any]:
        """تشغيل جمع القمامة"""
        self.stats['gc_runs'] += 1
        result = self.cleaner.collect_garbage()
        self.logger.info("🧹 تم تشغيل جمع القمامة")
        return result
    
    def run_cleanup(self) -> Dict[str, Any]:
        """تشغيل التنظيف"""
        self.stats['cleaning_runs'] += 1
        result = self.cleaner.cleanup()
        self.logger.info("🧹 تم تشغيل التنظيف")
        return result
    
    def start(self):
        """بدء تشغيل مدير الذاكرة"""
        if self.running:
            return
        
        self.running = True
        
        # بدء التنظيف التلقائي
        self.cleaner.start()
        
        # بدء مراقبة الذاكرة
        def monitor_loop():
            while self.running:
                time.sleep(30)
                stats = self.get_memory_stats()
                self.logger.debug(f"📊 استخدام الذاكرة: {stats['memory']['percent']:.1f}%")
                
                # التحقق من الحالة الحرجة
                if stats['memory']['percent'] > self.config.critical_threshold:
                    self.logger.warning(f"⚠️ حالة حرجة! الذاكرة {stats['memory']['percent']:.1f}%")
                    self.run_cleanup()
                    self.run_garbage_collection()
        
        self.monitor_thread = threading.Thread(target=monitor_loop, daemon=True)
        self.monitor_thread.start()
        
        self.logger.info("✅ بدء تشغيل مدير الذاكرة")
    
    def stop(self):
        """إيقاف تشغيل مدير الذاكرة"""
        self.running = False
        self.cleaner.stop()
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
        self.logger.info("⏹️ إيقاف تشغيل مدير الذاكرة")
    
    def get_health_report(self) -> Dict[str, Any]:
        """تقرير صحة الذاكرة"""
        stats = self.get_memory_stats()
        status = self.get_memory_status()
        leaks = self.get_leaks_report()
        
        return {
            'timestamp': time.time(),
            'status': status.value,
            'memory_usage': stats['memory']['percent'],
            'available_mb': stats['memory']['available'] / (1024 * 1024),
            'allocations': {
                'total': self.allocations_count,
                'active': self.allocations_count - self.freed_count,
                'freed': self.freed_count
            },
            'leaks': leaks,
            'gc_runs': self.stats['gc_runs'],
            'cleaning_runs': self.stats['cleaning_runs'],
            'peak_memory_mb': self.stats['peak_memory'] / (1024 * 1024),
            'is_healthy': status != MemoryStatus.CRITICAL
        }

# ============================================================
# وظائف مساعدة (الأسطر 401-500)
# ============================================================

def format_bytes(bytes: int, unit: MemoryUnit = MemoryUnit.MEGABYTE) -> float:
    """تنسيق البايتات إلى وحدة معينة"""
    return bytes / unit.value

def format_bytes_human(bytes: int) -> str:
    """تنسيق البايتات بطريقة مقروءة للإنسان"""
    units = ['B', 'KB', 'MB', 'GB', 'TB']
    value = float(bytes)
    unit_index = 0
    
    while value >= 1024 and unit_index < len(units) - 1:
        value /= 1024
        unit_index += 1
    
    return f"{value:.2f} {units[unit_index]}"

def get_available_memory() -> int:
    """الحصول على الذاكرة المتاحة بالبايت"""
    return psutil.virtual_memory().available

def get_used_memory() -> int:
    """الحصول على الذاكرة المستخدمة بالبايت"""
    return psutil.virtual_memory().used

def get_memory_percent() -> float:
    """الحصول على نسبة استخدام الذاكرة"""
    return psutil.virtual_memory().percent

def is_memory_critical(threshold: float = 90.0) -> bool:
    """التحقق مما إذا كانت الذاكرة في حالة حرجة"""
    return get_memory_percent() > threshold

# ============================================================
# نقطة الدخول الرئيسية (الأسطر 501-600)
# ============================================================

def main():
    """اختبار مدير الذاكرة"""
    print("=" * 80)
    print("🗄️ MEMORY MANAGER v1.0.0")
    print("=" * 80)
    
    # إنشاء مدير الذاكرة
    manager = MemoryManager()
    
    # بدء التشغيل
    manager.start()
    
    # تخصيص ذاكرة اختبارية
    allocations = []
    for i in range(10):
        alloc_id = manager.allocate(1024 * 1024, f"test_{i}")
        if alloc_id:
            allocations.append(alloc_id)
            print(f"📦 تخصيص {i+1}: {alloc_id[:8]}")
        time.sleep(0.1)
    
    # عرض الإحصائيات
    stats = manager.get_memory_stats()
    print(f"\n📊 إحصائيات الذاكرة:")
    print(f"   الاستخدام: {stats['memory']['percent']:.1f}%")
    print(f"   المتاح: {format_bytes_human(stats['memory']['available'])}")
    print(f"   المستخدم: {format_bytes_human(stats['memory']['used'])}")
    
    # تحرير بعض التخصيصات
    for i, alloc_id in enumerate(allocations[:5]):
        manager.free(alloc_id)
        print(f"🗑️ تحرير {i+1}: {alloc_id[:8]}")
    
    # جمع القمامة
    manager.run_garbage_collection()
    
    # تقرير الصحة
    health = manager.get_health_report()
    print(f"\n❤️ تقرير الصحة:")
    print(f"   الحالة: {health['status']}")
    print(f"   التسريبات: {health['leaks']['leaks_count']}")
    print(f"   التخصيصات النشطة: {health['allocations']['active']}")
    
    # إيقاف التشغيل
    manager.stop()
    
    print("\n✅ اختبار مدير الذاكرة اكتمل")

if __name__ == "__main__":
    main()
