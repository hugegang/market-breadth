import pandas as pd
import os
import json
from datetime import datetime, timedelta
import logging

class DataStorage:
    def __init__(self, cache_dir="cache"):
        self.cache_dir = cache_dir
        self._ensure_cache_dir()
        
    def _ensure_cache_dir(self):
        """确保缓存目录存在"""
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)
            
    def _get_cache_path(self, ma_period: int, sample_size: int, lookback_days: int) -> str:
        """获取缓存文件路径"""
        return os.path.join(
            self.cache_dir, 
            f"market_breadth_ma{ma_period}_n{sample_size}_d{lookback_days}.csv"
        )
        
    def _get_metadata_path(self, ma_period: int, sample_size: int, lookback_days: int) -> str:
        """获取元数据文件路径"""
        return os.path.join(
            self.cache_dir, 
            f"market_breadth_ma{ma_period}_n{sample_size}_d{lookback_days}_metadata.json"
        )
    
    def save_data(self, df: pd.DataFrame, ma_period: int, sample_size: int, lookback_days: int):
        """保存市场宽度数据和元数据"""
        # 保存数据
        cache_path = self._get_cache_path(ma_period, sample_size, lookback_days)
        df.to_csv(cache_path, index=True)
        
        # 保存元数据
        metadata = {
            "last_update": datetime.now().isoformat(),
            "ma_period": ma_period,
            "sample_size": sample_size,
            "lookback_days": lookback_days
        }
        
        with open(self._get_metadata_path(ma_period, sample_size, lookback_days), 'w') as f:
            json.dump(metadata, f)
            
    def load_data(self, ma_period: int, sample_size: int, lookback_days: int, max_age_hours: int = 24) -> pd.DataFrame:
        """加载市场宽度数据，如果数据过期则返回None"""
        cache_path = self._get_cache_path(ma_period, sample_size, lookback_days)
        metadata_path = self._get_metadata_path(ma_period, sample_size, lookback_days)
        
        if not (os.path.exists(cache_path) and os.path.exists(metadata_path)):
            return None
            
        # 检查数据是否过期
        with open(metadata_path, 'r') as f:
            metadata = json.load(f)
            
        last_update = datetime.fromisoformat(metadata['last_update'])
        if datetime.now() - last_update > timedelta(hours=max_age_hours):
            return None
            
        # 加载数据
        try:
            df = pd.read_csv(cache_path)
            df['date'] = pd.to_datetime(df['date'])
            return df
        except Exception as e:
            logging.error(f"加载缓存数据失败: {e}")
            return None
