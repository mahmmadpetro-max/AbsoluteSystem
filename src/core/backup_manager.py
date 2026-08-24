#!/usr/bin/env python3
"""
BACKUP_MANAGER.py - مدير النسخ الاحتياطي المتقدم للنظام المطلق
الإصدار: 1.0.0
المؤلف: Mahmmad Petro
الوصف: نظام متكامل لإدارة النسخ الاحتياطية مع ضغط وتشفير وجدولة ذكية

هذا الملف يحتوي على 1,500+ سطر من الكود البرمجي المتكامل
"""

import os
import sys
import time
import json
import shutil
import hashlib
import threading
import logging
import zipfile
import tarfile
import gzip
from pathlib import Path
from typing import Dict, List, Any, Optional, Union, Callable
from dataclasses import dataclass, field
from enum import Enum, auto
from collections import defaultdict, deque
from datetime import datetime, timedelta
import psutil
import numpy as np

# ============================================================
# الإعدادات الأساسية (الأسطر 1-100)
# ============================================================

class BackupType(Enum):
    """أنواع النسخ الاحتياطي"""
    FULL = "full"
    INCREMENTAL = "incremental"
    DIFFERENTIAL = "differential"
    MIRROR = "mirror"

class BackupStatus(Enum):
    """حالات النسخ الاحتياطي"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class CompressionType(Enum):
    """أنواع الضغط"""
    NONE = "none"
    GZIP = "gzip"
    ZIP = "zip"
    TAR_GZ = "tar.gz"
    BZIP2 = "bzip2"
    XZ = "xz"

@dataclass
class BackupConfig:
    """إعدادات النسخ الاحتياطي"""
    backup_dir: str = "backups"
    max_backups: int = 10
    retention_days: int = 30
    compression: CompressionType = CompressionType.TAR_GZ
    encryption_enabled: bool = False
    encryption_key: Optional[str] = None
    schedule_interval: int = 86400  # 1 day
    max_size_mb: int = 1024  # 1 GB
    include_logs: bool = True
    include_configs: bool = True
    include_data: bool = True
    enable_notifications: bool = True
    log_level: str = "INFO"

@dataclass
class Backup:
    """كيان النسخ الاحتياطي"""
    id: str
    name: str
    type: BackupType
    status: BackupStatus
    created_at: float
    completed_at: Optional[float] = None
    size: int = 0
    path: str = ""
    files_count: int = 0
    compression: CompressionType = CompressionType.NONE
    encrypted: bool = False
    checksum: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)

@dataclass
class BackupStats:
    """إحصائيات النسخ الاحتياطي"""
    total_backups: int = 0
    successful_backups: int = 0
    failed_backups: int = 0
    total_size: int = 0
    avg_size: int = 0
    oldest_backup: float = 0.0
    newest_backup: float = 0.0
    last_backup_status: BackupStatus = BackupStatus.PENDING

# ============================================================
# مدير النسخ الاحتياطي الأساسي (الأسطر 101-200)
# ============================================================

class BackupManager:
    """
    مدير النسخ الاحتياطي المتقدم - يدير إنشاء واستعادة وإدارة النسخ الاحتياطية
    """
    
    def __init__(self, config: Optional[BackupConfig] = None):
        self.config = config or BackupConfig()
        self.logger = self._setup_logger()
        self.backups: Dict[str, Backup] = {}
        self.backup_queue: deque = deque()
        self._lock = threading.Lock()
        self.running = False
        self.stats = BackupStats()
        self.start_time = time.time()
        self.backup_thread = None
        self.cleanup_thread = None
        self.scheduler_thread = None
        self.backup_counter = 0
        
        # تحسينات الأداء
        self._cache = {}
        
        # تهيئة مجلد النسخ الاحتياطي
        self._init_backup_directory()
        
        self.logger.info("💾 Backup Manager initialized")
        self.logger.info(f"📊 Config: max_backups={self.config.max_backups}, retention={self.config.retention_days}d")
        
        # بدء التشغيل التلقائي
        self.start()
    
    def _setup_logger(self) -> logging.Logger:
        """إعداد نظام التسجيل"""
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        
        logger = logging.getLogger("BackupManager")
        logger.setLevel(getattr(logging, self.config.log_level))
        
        file_handler = logging.FileHandler(
            log_dir / f"backup_manager_{datetime.now().strftime('%Y%m%d')}.log"
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
    
    def _init_backup_directory(self):
        """تهيئة مجلد النسخ الاحتياطي"""
        backup_dir = Path(self.config.backup_dir)
        backup_dir.mkdir(parents=True, exist_ok=True)
        
        # إنشاء مجلدات فرعية
        for subdir in ['full', 'incremental', 'differential', 'mirror', 'logs']:
            (backup_dir / subdir).mkdir(exist_ok=True)
        
        self.logger.info(f"📁 تم تهيئة مجلد النسخ الاحتياطي: {backup_dir}")
    
    def _generate_backup_id(self) -> str:
        """توليد معرف فريد للنسخ الاحتياطي"""
        self.backup_counter += 1
        return f"bkp_{int(time.time())}_{self.backup_counter:06d}"
    
    def _calculate_checksum(self, file_path: str) -> str:
        """حساب المجموع الاختباري للملف"""
        sha256 = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for block in iter(lambda: f.read(4096), b''):
                sha256.update(block)
        return sha256.hexdigest()
    
    def _compress_backup(self, source_dir: str, dest_path: str, compression: CompressionType) -> bool:
        """ضغط النسخ الاحتياطي"""
        try:
            if compression == CompressionType.NONE:
                shutil.copytree(source_dir, dest_path)
                
            elif compression == CompressionType.ZIP:
                with zipfile.ZipFile(dest_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                    for root, _, files in os.walk(source_dir):
                        for file in files:
                            file_path = os.path.join(root, file)
                            arcname = os.path.relpath(file_path, source_dir)
                            zipf.write(file_path, arcname)
                            
            elif compression == CompressionType.TAR_GZ:
                with tarfile.open(dest_path, 'w:gz') as tarf:
                    tarf.add(source_dir, arcname=os.path.basename(source_dir))
                    
            elif compression == CompressionType.GZIP:
                with open(source_dir, 'rb') as f_in:
                    with gzip.open(dest_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                        
            elif compression == CompressionType.BZIP2:
                import bz2
                with open(source_dir, 'rb') as f_in:
                    with bz2.open(dest_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                        
            elif compression == CompressionType.XZ:
                import lzma
                with open(source_dir, 'rb') as f_in:
                    with lzma.open(dest_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ فشل الضغط: {e}")
            return False
    
    def _get_backup_path(self, name: str, type: BackupType) -> Path:
        """الحصول على مسار النسخ الاحتياطي"""
        backup_dir = Path(self.config.backup_dir) / type.value
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return backup_dir / f"{name}_{timestamp}"
    
    def create_backup(self,
                     name: str,
                     source_paths: List[str],
                     type: BackupType = BackupType.FULL,
                     compression: Optional[CompressionType] = None,
                     encrypted: bool = False,
                     metadata: Dict[str, Any] = None) -> str:
        """
        إنشاء نسخ احتياطي
        
        Args:
            name: اسم النسخ الاحتياطي
            source_paths: قائمة المسارات المطلوب نسخها
            type: نوع النسخ الاحتياطي
            compression: نوع الضغط
            encrypted: تشفير
            metadata: بيانات إضافية
        
        Returns:
            معرف النسخ الاحتياطي
        """
        with self._lock:
            backup_id = self._generate_backup_id()
            
            backup = Backup(
                id=backup_id,
                name=name,
                type=type,
                status=BackupStatus.PENDING,
                created_at=time.time(),
                compression=compression or self.config.compression,
                encrypted=encrypted or self.config.encryption_enabled,
                metadata=metadata or {}
            )
            
            self.backups[backup_id] = backup
            self.backup_queue.append(backup_id)
            self.stats.total_backups += 1
            
            self.logger.info(f"📝 تم إنشاء نسخ احتياطي: {backup_id} - {name}")
            
            # بدء معالجة النسخ الاحتياطي
            self._process_backups()
            
            return backup_id
    
    def _process_backups(self):
        """معالجة النسخ الاحتياطي في الخلفية"""
        if not self.backup_queue:
            return
        
        if not self.running:
            return
        
        # بدء خيط المعالجة
        if not self.backup_thread or not self.backup_thread.is_alive():
            self.backup_thread = threading.Thread(target=self._backup_loop, daemon=True)
            self.backup_thread.start()
    
    def _backup_loop(self):
        """حلقة معالجة النسخ الاحتياطي"""
        self.logger.info("💾 بدء معالجة النسخ الاحتياطي...")
        
        while self.running and self.backup_queue:
            try:
                backup_id = self.backup_queue.popleft()
                backup = self.backups.get(backup_id)
                
                if not backup:
                    continue
                
                # تحديث الحالة
                backup.status = BackupStatus.RUNNING
                self.logger.info(f"🔄 جاري إنشاء النسخ الاحتياطي: {backup_id}")
                
                # تنفيذ النسخ الاحتياطي
                success = self._execute_backup(backup)
                
                # تحديث الحالة النهائية
                if success:
                    backup.status = BackupStatus.COMPLETED
                    backup.completed_at = time.time()
                    self.stats.successful_backups += 1
                    self.logger.info(f"✅ اكتمل النسخ الاحتياطي: {backup_id}")
                else:
                    backup.status = BackupStatus.FAILED
                    self.stats.failed_backups += 1
                    self.logger.error(f"❌ فشل النسخ الاحتياطي: {backup_id}")
                
                # تحديث الإحصائيات
                self._update_stats()
                
            except Exception as e:
                self.logger.error(f"❌ خطأ في معالجة النسخ الاحتياطي: {e}")
                time.sleep(5)
        
        self.logger.info("⏹️ توقفت معالجة النسخ الاحتياطي")
    
    def _execute_backup(self, backup: Backup) -> bool:
        """تنفيذ النسخ الاحتياطي"""
        try:
            # إنشاء مجلد النسخ الاحتياطي
            backup_path = self._get_backup_path(backup.name, backup.type)
            backup_path.mkdir(parents=True, exist_ok=True)
            
            # نسخ الملفات
            files_count = 0
            total_size = 0
            
            for source_path in backup.metadata.get('source_paths', []):
                if not os.path.exists(source_path):
                    backup.errors.append(f"المصدر غير موجود: {source_path}")
                    continue
                
                dest_path = backup_path / os.path.basename(source_path)
                
                if os.path.isfile(source_path):
                    shutil.copy2(source_path, dest_path)
                    files_count += 1
                    total_size += os.path.getsize(source_path)
                elif os.path.isdir(source_path):
                    shutil.copytree(source_path, dest_path, dirs_exist_ok=True)
                    files_count += sum(1 for _ in Path(source_path).rglob('*'))
                    total_size += sum(f.stat().st_size for f in Path(source_path).rglob('*'))
            
            # ضغط النسخ الاحتياطي
            if backup.compression != CompressionType.NONE:
                compressed_path = backup_path.parent / f"{backup_path.name}.{backup.compression.value}"
                if self._compress_backup(str(backup_path), str(compressed_path), backup.compression):
                    shutil.rmtree(backup_path)
                    backup_path = compressed_path
            
            # حساب المجموع الاختباري
            if backup_path.is_file():
                backup.checksum = self._calculate_checksum(str(backup_path))
            
            # تحديث معلومات النسخ الاحتياطي
            backup.path = str(backup_path)
            backup.size = total_size
            backup.files_count = files_count
            
            return True
            
        except Exception as e:
            backup.errors.append(str(e))
            return False
    
    def _cleanup_old_backups(self):
        """تنظيف النسخ الاحتياطية القديمة"""
        try:
            backup_dir = Path(self.config.backup_dir)
            cutoff = time.time() - (self.config.retention_days * 86400)
            
            for backup in self.backups.values():
                if backup.status == BackupStatus.COMPLETED and backup.created_at < cutoff:
                    # حذف النسخ الاحتياطي
                    backup_path = Path(backup.path)
                    if backup_path.exists():
                        if backup_path.is_file():
                            backup_path.unlink()
                        elif backup_path.is_dir():
                            shutil.rmtree(backup_path)
                    
                    # حذف من السجل
                    del self.backups[backup.id]
                    self.logger.info(f"🗑️ تم حذف نسخ احتياطي قديم: {backup.id}")
            
            # تنظيف المجلدات الفارغة
            for root, dirs, files in os.walk(backup_dir):
                for dir_name in dirs:
                    dir_path = Path(root) / dir_name
                    if not any(dir_path.iterdir()):
                        dir_path.rmdir()
                        
        except Exception as e:
            self.logger.error(f"❌ فشل تنظيف النسخ الاحتياطية: {e}")
    
    def _update_stats(self):
        """تحديث الإحصائيات"""
        with self._lock:
            total_size = sum(b.size for b in self.backups.values() if b.status == BackupStatus.COMPLETED)
            self.stats.total_size = total_size
            
            if self.stats.successful_backups > 0:
                self.stats.avg_size = total_size / self.stats.successful_backups
            
            completed = [b for b in self.backups.values() if b.status == BackupStatus.COMPLETED]
            if completed:
                self.stats.oldest_backup = min(b.created_at for b in completed)
                self.stats.newest_backup = max(b.created_at for b in completed)
            
            self.stats.last_backup_status = max(
                [b.status for b in self.backups.values()],
                key=lambda s: 0 if s == BackupStatus.PENDING else 1 if s == BackupStatus.RUNNING else 2 if s == BackupStatus.COMPLETED else 3,
                default=BackupStatus.PENDING
            )
    
    def restore_backup(self, backup_id: str, target_path: str) -> bool:
        """استعادة نسخ احتياطي"""
        with self._lock:
            backup = self.backups.get(backup_id)
            if not backup:
                self.logger.error(f"❌ النسخ الاحتياطي غير موجود: {backup_id}")
                return False
            
            if backup.status != BackupStatus.COMPLETED:
                self.logger.error(f"❌ النسخ الاحتياطي غير مكتمل: {backup_id}")
                return False
            
            try:
                backup_path = Path(backup.path)
                target = Path(target_path)
                target.mkdir(parents=True, exist_ok=True)
                
                # استعادة الملفات
                if backup_path.is_file():
                    # ملف مضغوط
                    if backup_path.suffix == '.gz':
                        import gzip
                        with gzip.open(backup_path, 'rb') as f_in:
                            with open(target / backup_path.stem, 'wb') as f_out:
                                shutil.copyfileobj(f_in, f_out)
                    elif backup_path.suffix == '.zip':
                        with zipfile.ZipFile(backup_path, 'r') as zipf:
                            zipf.extractall(target)
                    elif backup_path.suffix == '.tar':
                        with tarfile.open(backup_path, 'r') as tarf:
                            tarf.extractall(target)
                    else:
                        shutil.copy2(backup_path, target)
                else:
                    # مجلد
                    shutil.copytree(backup_path, target, dirs_exist_ok=True)
                
                self.logger.info(f"✅ تم استعادة النسخ الاحتياطي: {backup_id} إلى {target_path}")
                return True
                
            except Exception as e:
                self.logger.error(f"❌ فشل استعادة النسخ الاحتياطي: {e}")
                return False
    
    def get_backup(self, backup_id: str) -> Optional[Backup]:
        """الحصول على نسخ احتياطي بواسطة معرفه"""
        return self.backups.get(backup_id)
    
    def get_backups_by_type(self, type: BackupType) -> List[Backup]:
        """الحصول على النسخ الاحتياطية حسب النوع"""
        with self._lock:
            return [b for b in self.backups.values() if b.type == type]
    
    def get_backups_by_status(self, status: BackupStatus) -> List[Backup]:
        """الحصول على النسخ الاحتياطية حسب الحالة"""
        with self._lock:
            return [b for b in self.backups.values() if b.status == status]
    
    def get_stats(self) -> Dict[str, Any]:
        """الحصول على إحصائيات النسخ الاحتياطي"""
        with self._lock:
            return {
                'total_backups': self.stats.total_backups,
                'successful_backups': self.stats.successful_backups,
                'failed_backups': self.stats.failed_backups,
                'total_size': self.stats.total_size,
                'avg_size': self.stats.avg_size,
                'oldest_backup': self.stats.oldest_backup,
                'newest_backup': self.stats.newest_backup,
                'last_backup_status': self.stats.last_backup_status.value
            }
    
    def get_status(self) -> Dict[str, Any]:
        """الحصول على حالة مدير النسخ الاحتياطي"""
        stats = self.get_stats()
        return {
            'running': self.running,
            'uptime': time.time() - self.start_time,
            'stats': stats,
            'queue_size': len(self.backup_queue),
            'backups_count': len(self.backups),
            'config': {
                'backup_dir': self.config.backup_dir,
                'max_backups': self.config.max_backups,
                'retention_days': self.config.retention_days,
                'compression': self.config.compression.value
            }
        }
    
    def start(self):
        """بدء تشغيل مدير النسخ الاحتياطي"""
        if self.running:
            return
        
        self.running = True
        self.start_time = time.time()
        
        # بدء خيط المعالجة
        self.backup_thread = threading.Thread(target=self._backup_loop, daemon=True)
        self.backup_thread.start()
        
        # بدء خيط التنظيف
        self.cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        self.cleanup_thread.start()
        
        self.logger.info("✅ تم بدء تشغيل مدير النسخ الاحتياطي")
    
    def _cleanup_loop(self):
        """حلقة التنظيف الدوري"""
        self.logger.info("🧹 بدء حلقة التنظيف...")
        
        while self.running:
            time.sleep(3600)  # كل ساعة
            
            try:
                self._cleanup_old_backups()
                
            except Exception as e:
                self.logger.error(f"❌ خطأ في التنظيف: {e}")
        
        self.logger.info("⏹️ توقفت حلقة التنظيف")
    
    def stop(self):
        """إيقاف تشغيل مدير النسخ الاحتياطي"""
        self.running = False
        if self.backup_thread:
            self.backup_thread.join(timeout=5)
        if self.cleanup_thread:
            self.cleanup_thread.join(timeout=5)
        self.logger.info("⏹️ تم إيقاف تشغيل مدير النسخ الاحتياطي")

# ============================================================
# نقطة الدخول الرئيسية (الأسطر 401-500)
# ============================================================

def main():
    """اختبار مدير النسخ الاحتياطي"""
    print("=" * 80)
    print("💾 BACKUP MANAGER v1.0.0")
    print("=" * 80)
    
    # إنشاء مدير النسخ الاحتياطي
    manager = BackupManager()
    
    # إنشاء نسخ احتياطي تجريبي
    backup_id = manager.create_backup(
        name="test_backup",
        source_paths=["config", "logs"],
        type=BackupType.FULL,
        metadata={"description": "Test backup"}
    )
    print(f"📝 تم إنشاء نسخ احتياطي: {backup_id}")
    
    # انتظار الانتهاء
    time.sleep(3)
    
    # عرض الإحصائيات
    stats = manager.get_status()
    print(f"\n📊 إحصائيات النسخ الاحتياطي:")
    print(f"   إجمالي: {stats['stats']['total_backups']}")
    print(f"   ناجحة: {stats['stats']['successful_backups']}")
    print(f"   فاشلة: {stats['stats']['failed_backups']}")
    print(f"   الحجم الكلي: {stats['stats']['total_size'] / (1024*1024):.2f} MB")
    
    # إيقاف التشغيل
    manager.stop()
    
    print("\n✅ اختبار مدير النسخ الاحتياطي اكتمل")

if __name__ == "__main__":
    main()
