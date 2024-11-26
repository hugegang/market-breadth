import yfinance as yf
import pandas as pd
from typing import List, Dict
import yaml
import logging
from datetime import datetime, timedelta
import concurrent.futures
import requests
import time

class DataFetcher:
    def __init__(self, config_path: str = "config/config.yaml"):
        """初始化数据获取器"""
        self.config = self._load_config(config_path)
        self.cache = {}
        self.last_update = {}
        logging.basicConfig(level=logging.INFO)
        self.session = requests.Session()
        self.error_count = {}
        
    def _load_config(self, config_path: str) -> Dict:
        """加载配置文件"""
        try:
            with open(config_path, 'r', encoding='utf-8') as file:
                return yaml.safe_load(file)
        except Exception as e:
            logging.error(f"加载配置文件失败: {e}")
            return {}
    
    def get_stock_data(self, symbol: str, period: int = 21) -> pd.DataFrame:
        """获取单个股票的历史数据"""
        try:
            # 检查缓存
            cache_key = f"{symbol}_{period}"
            if cache_key in self.cache:
                last_update = self.last_update.get(cache_key)
                if last_update and (datetime.now() - last_update).seconds < 3600:  # 1小时缓存
                    return self.cache[cache_key]
            
            # 获取新数据
            stock = yf.Ticker(symbol)
            # 获取足够长的历史数据以计算均线
            days_needed = period * 2  # 获取2倍周期的数据以确保有足够数据计算均线
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_needed)
            
            # 添加重试机制
            max_retries = 3
            retry_delay = 1
            for attempt in range(max_retries):
                try:
                    data = stock.history(start=start_date, end=end_date)
                    if not data.empty:
                        # 更新缓存
                        self.cache[cache_key] = data
                        self.last_update[cache_key] = datetime.now()
                        return data
                    break
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise e
                    time.sleep(retry_delay)
            
            logging.error(f"获取股票 {symbol} 数据失败: 数据为空")
            return pd.DataFrame()
                
        except Exception as e:
            logging.error(f"获取股票 {symbol} 数据失败: {e}")
            return pd.DataFrame()
    
    def get_stock_data_batch(self, symbols: List[str], progress_callback=None) -> Dict[str, pd.DataFrame]:
        """批量获取股票数据"""
        batch_size = 20  # 每批处理的股票数量
        results = {}
        
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i+batch_size]
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                future_to_symbol = {executor.submit(self.get_stock_data, symbol): symbol 
                                  for symbol in batch}
                
                completed = 0
                total = len(batch)
                
                for future in concurrent.futures.as_completed(future_to_symbol):
                    symbol = future_to_symbol[future]
                    completed += 1
                    
                    try:
                        data = future.result()
                        if not data.empty:
                            results[symbol] = data
                    except Exception as e:
                        logging.error(f"处理 {symbol} 时出错: {e}")
                    
                    if progress_callback:
                        progress_callback(completed / total)
            
            # 添加延迟以避免请求过快
            time.sleep(1)
        
        return results
    
    def get_sp500_components(self) -> List[str]:
        """获取标普500成分股列表"""
        try:
            url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
            tables = pd.read_html(url)
            df = tables[0]
            return df['Symbol'].tolist()
        except Exception as e:
            logging.error(f"获取标普500成分股失败: {e}")
            return []
    
    def get_index_data(self, index_name: str) -> Dict:
        """获取指数数据"""
        try:
            index_config = self.config['indices'][index_name]
            symbol = index_config['symbol']
            index = yf.Ticker(symbol)
            data = index.history(period="1d")
            return {
                'name': index_config['name'],
                'symbol': symbol,
                'data': data
            }
        except Exception as e:
            logging.error(f"获取指数 {index_name} 数据失败: {e}")
            return {}
