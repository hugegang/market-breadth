import pandas as pd
import logging
from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor
import numpy as np
from ..data.data_fetcher import DataFetcher
from ..data.data_storage import DataStorage
import pytz

class MarketBreadth:
    def __init__(self, data_fetcher: DataFetcher):
        self.data_fetcher = data_fetcher
        self.data_storage = DataStorage()
        logging.basicConfig(level=logging.INFO)
    
    def calculate_ma(self, df: pd.DataFrame, period: int) -> pd.Series:
        """计算移动平均线"""
        return df['Close'].rolling(window=period).mean()
    
    def analyze_stock(self, symbol: str, ma_period: int) -> Dict:
        """分析单个股票相对于均线的位置"""
        try:
            # 获取股票数据
            data = self.data_fetcher.get_stock_data(symbol, ma_period)
            if data.empty:
                return {'valid': False}
            
            # 计算均线
            ma = self.calculate_ma(data, ma_period)
            
            # 获取最新价格和均线值
            latest_price = data['Close'].iloc[-1]
            latest_ma = ma.iloc[-1]
            
            return {
                'valid': True,
                'price': latest_price,
                'ma': latest_ma,
                'above_ma': latest_price > latest_ma
            }
            
        except Exception as e:
            logging.error(f"分析股票 {symbol} 失败: {e}")
            return {'valid': False}
    
    def calculate_advance_decline_ratio(self, data: Dict[str, pd.DataFrame]) -> float:
        """计算上涨/下跌比率"""
        advances = 0
        declines = 0
        for symbol, df in data.items():
            if not df.empty:
                price_change = df['Close'].iloc[-1] - df['Close'].iloc[-2]
                if price_change > 0:
                    advances += 1
                elif price_change < 0:
                    declines += 1
        return advances / declines if declines > 0 else float('inf')
    
    def calculate_new_highs_lows(self, data: Dict[str, pd.DataFrame], period: int = 20) -> Dict:
        """计算新高新低比率"""
        # ... implementation ...
    
    def calculate_historical_breadth(self, symbols: List[str], ma_period: int, lookback_days: int = 1000) -> pd.DataFrame:
        """计算历史市场宽度数据"""
        try:
            # 尝试从缓存加载数据
            cached_data = self.data_storage.load_data(ma_period, len(symbols), lookback_days)
            if cached_data is not None:
                logging.info("使用缓存的市场宽度数据")
                return cached_data
                
            logging.info(f"开始计算历史市场宽度，样本数: {len(symbols)}, MA周期: {ma_period}, 回溯天数: {lookback_days}")
            
            # 获取所有股票的历史数据
            all_stocks_data = {}
            for symbol in symbols:
                data = self.data_fetcher.get_stock_data(symbol, lookback_days + ma_period)
                if not data.empty:
                    # 先移除时区信息
                    data.index = data.index.tz_localize(None)
                    all_stocks_data[symbol] = data
            
            logging.info(f"成功获取 {len(all_stocks_data)} 只股票的历史数据")

            # 创建日期范围（不带时区）
            end_date = pd.Timestamp.now().normalize()  # 获取当前日期（不带时间）
            dates = pd.date_range(end=end_date, periods=lookback_days, freq='B')
            breadth_data = []
            
            for date in dates:
                stocks_above_ma = 0
                valid_stocks = 0
                
                for symbol, data in all_stocks_data.items():
                    # 获取直到当前日期的数据
                    hist_data = data[data.index <= date]
                    if len(hist_data) >= ma_period:
                        valid_stocks += 1
                        # 计算MA
                        ma = self.calculate_ma(hist_data, ma_period)
                        if hist_data['Close'].iloc[-1] > ma.iloc[-1]:
                            stocks_above_ma += 1
                
                if valid_stocks > 0:
                    breadth_percentage = (stocks_above_ma / valid_stocks) * 100
                    breadth_data.append({
                        'date': date,
                        'breadth': breadth_percentage,
                        'stocks_above_ma': stocks_above_ma,
                        'valid_stocks': valid_stocks
                    })
            
            result_df = pd.DataFrame(breadth_data)
            
            # 保存计算结果到缓存
            self.data_storage.save_data(result_df, ma_period, len(symbols), lookback_days)
            
            logging.info(f"历史市场宽度计算完成，共 {len(result_df)} 个数据点")
            return result_df
            
        except Exception as e:
            logging.error(f"计算历史市场宽度失败: {e}")
            raise e
    
    def get_index_data(self, lookback_days: int = 1000) -> pd.DataFrame:
        """获取标普500指数数据"""
        try:
            index_symbol = "^GSPC"
            data = self.data_fetcher.get_stock_data(index_symbol, lookback_days)
            
            if not data.empty:
                # 确保索引没有时区信息
                data.index = data.index.tz_localize(None)
                # 修改：使用第一个收盘价作为基准计算变化百分比
                first_close = data['Close'].iloc[0]
                data['change_pct'] = ((data['Close'] - first_close) / first_close) * 100
                return data
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"获取标普500数据失败: {e}")
            return pd.DataFrame()
    
    def calculate_bullish_alignment(self, symbols: List[str]) -> Dict:
        """计算完全多头排列的股票比例"""
        try:
            bullish_count = 0
            valid_stocks = 0
            bullish_stocks = []  # 存储多头排列的股票代码
            
            # 获取足够长的历史数据以计算最长周期的均线
            lookback_days = 127 * 2  # 获取2倍的最长均线周期数据
            
            for symbol in symbols:
                try:
                    # 获取股票数据
                    data = self.data_fetcher.get_stock_data(symbol, lookback_days)
                    if not data.empty and len(data) >= 127:  # 确保有足够的数据计算均线
                        valid_stocks += 1
                        
                        # 计算三个均线
                        ma21 = self.calculate_ma(data, 21)
                        ma63 = self.calculate_ma(data, 63)
                        ma127 = self.calculate_ma(data, 127)
                        
                        # 获取最新价格和均线值
                        latest_price = data['Close'].iloc[-1]
                        latest_ma21 = ma21.iloc[-1]
                        latest_ma63 = ma63.iloc[-1]
                        latest_ma127 = ma127.iloc[-1]
                        
                        # 判断是否多头排列
                        if (latest_price > latest_ma21 > latest_ma63 > latest_ma127):
                            bullish_count += 1
                            bullish_stocks.append(symbol)
                
                except Exception as e:
                    logging.error(f"处理股票 {symbol} 时出错: {e}")
                    continue
            
            # 计算比例
            bullish_percentage = (bullish_count / valid_stocks * 100) if valid_stocks > 0 else 0
            
            return {
                'bullish_percentage': bullish_percentage,
                'bullish_count': bullish_count,
                'valid_stocks': valid_stocks,
                'bullish_stocks': bullish_stocks
            }
                
        except Exception as e:
            logging.error(f"计算多头排列比例时出错: {e}")
            return {
                'bullish_percentage': 0,
                'bullish_count': 0,
                'valid_stocks': 0,
                'bullish_stocks': []
            }
    
    def calculate_historical_bullish_alignment(self, symbols: List[str], lookback_days: int) -> pd.DataFrame:
        """计算历史多头排列比例"""
        try:
            # 创建日期范围（使用美国东部时间）
            end_date = pd.Timestamp.now(tz='America/New_York')
            start_date = end_date - pd.Timedelta(days=lookback_days)
            dates = pd.date_range(start=start_date, end=end_date, freq='B', tz='America/New_York')
            
            breadth_data = []
            
            for current_date in dates:
                bullish_count = 0
                valid_stocks = 0
                bullish_stocks = []
                
                for symbol in symbols:
                    try:
                        # 获取股票数据
                        data = self.data_fetcher.get_stock_data(symbol, 127 * 2)
                        
                        if data is None or data.empty:
                            continue
                        
                        # 确保索引有时区信息
                        if data.index.tz is None:
                            data.index = data.index.tz_localize('America/New_York')
                        elif data.index.tz != pytz.timezone('America/New_York'):
                            data.index = data.index.tz_convert('America/New_York')
                        
                        # 使用时区感知的时间戳进行过滤
                        data = data[data.index <= current_date]
                        
                        if len(data) >= 127:
                            valid_stocks += 1
                            
                            # 计算均线
                            ma21 = data['Close'].rolling(window=21).mean()
                            ma63 = data['Close'].rolling(window=63).mean()
                            ma127 = data['Close'].rolling(window=127).mean()
                            
                            if not ma21.empty and not ma63.empty and not ma127.empty:
                                latest_price = data['Close'].iloc[-1]
                                latest_ma21 = ma21.iloc[-1]
                                latest_ma63 = ma63.iloc[-1]
                                latest_ma127 = ma127.iloc[-1]
                                
                                if not pd.isna([latest_price, latest_ma21, latest_ma63, latest_ma127]).any():
                                    if (latest_price > latest_ma21 > latest_ma63 > latest_ma127):
                                        bullish_count += 1
                                        bullish_stocks.append(symbol)
                
                    except Exception as e:
                        logging.error(f"处理股票 {symbol} 在日期 {current_date} 时出错: {e}")
                        continue
                
                # 计算当天的多头排列比例
                breadth = (bullish_count / valid_stocks * 100) if valid_stocks > 0 else 0
                breadth_data.append({
                    'date': current_date.tz_convert(None),  # 移除时区信息用于存储
                    'breadth': breadth,
                    'bullish_count': bullish_count,
                    'valid_stocks': valid_stocks,
                    'bullish_stocks': ','.join(bullish_stocks) if bullish_stocks else ''
                })
            
            # 创建DataFrame
            df = pd.DataFrame(breadth_data)
            
            if not df.empty:
                # 确保日期列格式正确
                df['date'] = pd.to_datetime(df['date'])
            
            return df
            
        except Exception as e:
            logging.error(f"计算历史多头排列比例时出错: {e}")
            return pd.DataFrame()
