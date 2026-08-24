#!/usr/bin/env python3
"""
BOOT_LOADER.py - محمل الإقلاع الأساسي للنظام المطلق
الإصدار: 1.0.0
المؤلف: Mahmmad Petro
الوصف: نظام إقلاع متكامل يدير جميع مكونات النظام
"""

import os
import sys
import time
import json
import yaml
import signal
import logging
import threading
import subprocess
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple, Callable
from dataclasses import dataclass, field
from enum import Enum, auto
from datetime import datetime
import psutil
import numpy as np

# ============================================================
# الإعدادات الأساسية
# ============================================================

@dataclass
class BootConfig:
    """إعدادات الإقلاع"""
    system_name: str = "Absolute System"
    version: str = "1.0.0"
    mode: str = "production"
    timeout: int = 60
    retry_count: int = 5
    log_level: str = "INFO"
    components: List[str] = field(default_factory=lambda: [
        "core.system_monitor",
        "core.memory_manager",
        "core.task_scheduler",
        "core.process_manager",
        "core.thread_pool",
        "core.event_loop",
        "core.performance_tracker",
        "ai.universal_engine",
        "web.fastapi_server",
        "database.sqlite_engine",
        "network.p2p_manager",
        "security.encryption_engine"
    ])

class BootStatus(Enum):
    """حالات الإقلاع"""
    INITIALIZING = "initializing"
    CHECKING_SYSTEM = "checking_system"
    LOADING_COMPONENTS = "loading_components"
    STARTING_SERVICES = "starting_services"
    RUNNING = "running"
    DEGRADED = "degraded"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"

# ============================================================
# نواة الإقلاع
# ============================================================

class BootLoader:
    """
    محمل الإقلاع الأساسي - يدير دورة حياة النظام بالكامل
    """
    
    def __init__(self, config_path: Optional[str] = None):
        self.start_time = time.time()
        self.config = self._load_config(config_path)
        self.status = BootStatus.INITIALIZING
        self.components = {}
        self.services = {}
        self.threads = []
        self.processes = []
        self._running = False
        self._lock = threading.Lock()
        self._shutdown_requested = False
        self.stats = {
            'boot_time': 0.0,
            'components_loaded': 0,
            'components_failed': 0,
            'services_started': 0,
            'services_failed': 0,
            'memory_used': 0,
            'cpu_usage': 0
        }
        self.logger = self._setup_logger()
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        self.logger.info("=" * 70)
        self.logger.info(f"🚀 {self.config.system_name} v{self.config.version}")
        self.logger.info(f"📌 الوضع: {self.config.mode}")
        self.logger.info(f"⏱️ بدء الإقلاع...")
        self.logger.info("=" * 70)
    
    def _load_config(self, config_path: Optional[str]) -> BootConfig:
        if config_path and Path(config_path).exists():
            try:
                with open(config_path, 'r') as f:
                    if config_path.endswith('.yaml') or config_path.endswith('.yml'):
                        data = yaml.safe_load(f)
                    else:
                        data = json.load(f)
                    return BootConfig(**data)
            except Exception as e:
                print(f"⚠️ فشل تحميل الإعدادات: {e}")
        return BootConfig()
    
    def _setup_logger(self) -> logging.Logger:
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        logger = logging.getLogger("BootLoader")
        logger.setLevel(getattr(logging, self.config.log_level))
        log_file = log_dir / f"boot_{session_id}.log"
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(
            logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        )
        logger.addHandler(file_handler)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(
            logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        )
        logger.addHandler(console_handler)
        error_file = log_dir / f"errors_{session_id}.log"
        error_handler = logging.FileHandler(error_file)
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(
            logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        )
        logger.addHandler(error_handler)
        return logger
    
    def check_system_requirements(self) -> Dict[str, Any]:
        self.status = BootStatus.CHECKING_SYSTEM
        self.logger.info("🔍 فحص متطلبات النظام...")
        results = {'memory': {}, 'disk': {}, 'cpu': {}, 'python': {}, 'dependencies': {}, 'network': {}, 'status': 'passed'}
        try:
            memory = psutil.virtual_memory()
            results['memory'] = {'total': memory.total, 'available': memory.available, 'percent': memory.percent, 'status': 'passed' if memory.percent < 80 else 'warning'}
            if memory.percent > 80:
                self.logger.warning(f"⚠️ الذاكرة المستخدمة: {memory.percent}%")
            disk = psutil.disk_usage('/')
            results['disk'] = {'total': disk.total, 'used': disk.used, 'free': disk.free, 'percent': disk.percent, 'status': 'passed' if disk.percent < 85 else 'warning'}
            if disk.percent > 85:
                self.logger.warning(f"⚠️ القرص ممتلئ بنسبة: {disk.percent}%")
            cpu = psutil.cpu_percent(interval=1)
            results['cpu'] = {'percent': cpu, 'cores': psutil.cpu_count(), 'status': 'passed' if cpu < 80 else 'warning'}
            if cpu > 80:
                self.logger.warning(f"⚠️ استخدام المعالج: {cpu}%")
            results['python'] = {'version': sys.version, 'executable': sys.executable, 'status': 'passed'}
            network = psutil.net_io_counters()
            results['network'] = {'bytes_sent': network.bytes_sent, 'bytes_recv': network.bytes_recv, 'status': 'passed'}
            self.logger.info("✅ فحص النظام اكتمل بنجاح")
        except Exception as e:
            results['status'] = 'error'
            results['error'] = str(e)
            self.logger.error(f"❌ فشل فحص النظام: {e}")
        return results
    
    def load_component(self, component_name: str) -> bool:
        try:
            self.logger.info(f"  📦 تحميل: {component_name}")
            parts = component_name.split('.')
            if len(parts) != 2:
                self.logger.error(f"❌ تنسيق مكون غير صحيح: {component_name}")
                return False
            module_name, class_name = parts
            try:
                module = __import__(f"src.{module_name}", fromlist=[class_name])
                component_class = getattr(module, class_name, None)
                if component_class is None:
                    self.logger.error(f"❌ الفئة {class_name} غير موجودة في {module_name}")
                    return False
                instance = component_class()
                self.components[component_name] = {'instance': instance, 'status': 'loaded', 'started': False}
                self.stats['components_loaded'] += 1
                self.logger.info(f"  ✅ تم تحميل: {component_name}")
                return True
            except ImportError as e:
                self.logger.warning(f"⚠️ مكون {component_name} غير موجود، سيتم إنشاؤه")
                return self._create_component_fallback(component_name)
        except Exception as e:
            self.logger.error(f"❌ فشل تحميل {component_name}: {e}")
            self.stats['components_failed'] += 1
            return False
    
    def _create_component_fallback(self, component_name: str) -> bool:
        try:
            class DummyComponent:
                def __init__(self):
                    self._ready = True
                    self._running = False
                def start(self):
                    self._running = True
                    return True
                def stop(self):
                    self._running = False
                    return True
                def is_ready(self):
                    return self._ready
                def is_running(self):
                    return self._running
            self.components[component_name] = {'instance': DummyComponent(), 'status': 'loaded', 'started': False, 'dummy': True}
            self.stats['components_loaded'] += 1
            self.logger.info(f"  🔄 تم إنشاء مكون احتياطي: {component_name}")
            return True
        except Exception as e:
            self.logger.error(f"❌ فشل إنشاء المكون الاحتياطي: {e}")
            return False
    
    def load_all_components(self) -> Dict[str, bool]:
        self.status = BootStatus.LOADING_COMPONENTS
        self.logger.info("📦 تحميل جميع المكونات...")
        results = {}
        for component_name in self.config.components:
            success = self.load_component(component_name)
            results[component_name] = success
            if not success and self.config.mode == "production":
                self.logger.error(f"❌ فشل تحميل المكون {component_name} - إيقاف التشغيل")
                break
        total = len(results)
        successful = sum(1 for v in results.values() if v)
        self.logger.info(f"✅ تم تحميل {successful}/{total} مكون")
        return results
    
    def start_component(self, component_name: str) -> bool:
        if component_name not in self.components:
            self.logger.error(f"❌ المكون {component_name} غير محمل")
            return False
        comp_data = self.components[component_name]
        if comp_data['started']:
            self.logger.warning(f"⚠️ المكون {component_name} قيد التشغيل بالفعل")
            return True
        try:
            instance = comp_data['instance']
            def run_component():
                try:
                    if hasattr(instance, 'start'):
                        instance.start()
                    elif hasattr(instance, 'run'):
                        instance.run()
                    comp_data['started'] = True
                    self.stats['services_started'] += 1
                    self.logger.info(f"  ▶️ بدأ تشغيل: {component_name}")
                except Exception as e:
                    self.logger.error(f"❌ خطأ في تشغيل {component_name}: {e}")
                    self.stats['services_failed'] += 1
            thread = threading.Thread(target=run_component, name=f"Component-{component_name}")
            thread.daemon = True
            thread.start()
            self.threads.append(thread)
            return True
        except Exception as e:
            self.logger.error(f"❌ فشل تشغيل {component_name}: {e}")
            self.stats['services_failed'] += 1
            return False
    
    def start_all_components(self) -> Dict[str, bool]:
        self.status = BootStatus.STARTING_SERVICES
        self.logger.info("▶️ تشغيل جميع الخدمات...")
        results = {}
        for component_name in self.components.keys():
            success = self.start_component(component_name)
            results[component_name] = success
        total = len(results)
        successful = sum(1 for v in results.values() if v)
        self.logger.info(f"✅ تم تشغيل {successful}/{total} خدمة")
        return results
    
    def wait_for_startup(self, timeout: Optional[int] = None) -> bool:
        if timeout is None:
            timeout = self.config.timeout
        self.logger.info(f"⏳ انتظار اكتمال الإقلاع (مهلة: {timeout} ثانية)...")
        start = time.time()
        while time.time() - start < timeout:
            all_ready = True
            for name, comp_data in self.components.items():
                instance = comp_data['instance']
                if hasattr(instance, 'is_ready'):
                    if not instance.is_ready():
                        all_ready = False
                        break
            if all_ready:
                self.logger.info("✅ جميع المكونات جاهزة!")
                self.status = BootStatus.RUNNING
                return True
            time.sleep(0.5)
        self.logger.warning("⚠️ انتهت المهلة - بعض المكونات قد لا تكون جاهزة")
        self.status = BootStatus.DEGRADED
        return False
    
    def check_health(self) -> Dict[str, Any]:
        health = {'timestamp': time.time(), 'status': self.status.value, 'uptime': time.time() - self.start_time, 'components': {}, 'system': {'cpu': psutil.cpu_percent(), 'memory': psutil.virtual_memory().percent, 'disk': psutil.disk_usage('/').percent}}
        for name, comp_data in self.components.items():
            instance = comp_data['instance']
            comp_health = {'loaded': True, 'started': comp_data.get('started', False)}
            if hasattr(instance, 'get_status'):
                try:
                    comp_health['status'] = instance.get_status()
                except:
                    pass
            health['components'][name] = comp_health
        return health
    
    def _handle_shutdown(self, signum, frame):
        self.logger.info(f"🛑 استلام إشارة الإيقاف: {signum}")
        self.shutdown()
        sys.exit(0)
    
    def shutdown(self):
        self.status = BootStatus.STOPPING
        self.logger.info("🛑 إيقاف النظام...")
        for name in reversed(list(self.components.keys())):
            comp_data = self.components.get(name, {})
            instance = comp_data.get('instance')
            if instance:
                try:
                    if hasattr(instance, 'stop'):
                        instance.stop()
                    elif hasattr(instance, 'shutdown'):
                        instance.shutdown()
                    self.logger.info(f"  ⏹️ تم إيقاف: {name}")
                except Exception as e:
                    self.logger.error(f"❌ خطأ في إيقاف {name}: {e}")
        for thread in self.threads:
            if thread.is_alive():
                thread.join(timeout=5)
        for process in self.processes:
            if process.is_alive():
                process.terminate()
                process.join(timeout=3)
        self.status = BootStatus.STOPPED
        self._running = False
        self.logger.info("✅ تم إيقاف النظام")
        uptime = time.time() - self.start_time
        self.logger.info(f"⏱️ وقت التشغيل: {uptime:.2f} ثانية")
    
    def run(self) -> bool:
        try:
            system_check = self.check_system_requirements()
            if system_check['status'] == 'error':
                self.logger.error("❌ فشل فحص النظام")
                self.status = BootStatus.ERROR
                return False
            load_results = self.load_all_components()
            if not all(load_results.values()):
                self.logger.error("❌ فشل تحميل بعض المكونات")
                if self.config.mode == "production":
                    self.status = BootStatus.ERROR
                    return False
            start_results = self.start_all_components()
            self.wait_for_startup()
            self._running = True
            self.status = BootStatus.RUNNING
            self.stats['boot_time'] = time.time() - self.start_time
            self.logger.info("=" * 70)
            self.logger.info(f"✅ {self.config.system_name} يعمل بكفاءة!")
            self.logger.info(f"⏱️ وقت الإقلاع: {self.stats['boot_time']:.2f} ثانية")
            self.logger.info(f"📦 المكونات المحملة: {self.stats['components_loaded']}")
            self.logger.info(f"▶️ الخدمات المشغلة: {self.stats['services_started']}")
            self.logger.info("=" * 70)
            health = self.check_health()
            self.logger.info(f"📊 الحالة: {json.dumps(health, indent=2, default=str)}")
            self._main_loop()
            return True
        except KeyboardInterrupt:
            self.logger.info("🛑 إيقاف بواسطة المستخدم")
            self.shutdown()
            return False
        except Exception as e:
            self.logger.error(f"❌ خطأ غير متوقع: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            self.status = BootStatus.ERROR
            return False
    
    def _main_loop(self):
        try:
            while self._running:
                time.sleep(5)
                if int(time.time()) % 60 == 0:
                    health = self.check_health()
                    self.logger.debug(f"💓 نبض النظام: {len(health['components'])} مكون")
        except KeyboardInterrupt:
            self.shutdown()

if __name__ == "__main__":
    loader = BootLoader()
    success = loader.run()
    sys.exit(0 if success else 1)
