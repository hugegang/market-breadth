import pandas as pd
import numpy as np
from typing import Dict, List

class MarketAnalysis:
    def __init__(self):
        self.analysis_rules = {
            'breadth_levels': {
                'extreme_oversold': 20,
                'oversold': 40,
                'neutral': 60,
                'overbought': 80
            },
            'divergence_threshold': 10  # 背离判断阈值
        }
    
    def analyze_market_condition(self, breadth_df: pd.DataFrame, index_df: pd.DataFrame, bullish_alignment: dict = None) -> dict:
        """分析市场状况并生成报告"""
        current_breadth = breadth_df['breadth'].iloc[-1]
        
        # 获取最近20个交易日的数据
        recent_breadth = breadth_df.tail(20)
        recent_index = index_df.tail(20)
        
        analysis = {
            'market_status': self._get_market_status(current_breadth),
            'trend': self._analyze_trend(recent_breadth),
            'divergence': self._check_divergence(recent_breadth, recent_index),
            'signals': self._generate_signals(current_breadth, recent_breadth),
            'risk_level': self._assess_risk(recent_breadth, recent_index)
        }
        
        # 添加多头排列分析
        if bullish_alignment:
            analysis['bullish_alignment'] = self._analyze_bullish_alignment(bullish_alignment['bullish_percentage'])
        
        return analysis
    
    def _get_market_status(self, breadth: float) -> dict:
        """判断市场状态"""
        if breadth <= self.analysis_rules['breadth_levels']['extreme_oversold']:
            return {
                'status': '极度超卖',
                'description': '市场处于极度超卖状态，可能出现反弹机会',
                'alert_level': 'warning'
            }
        elif breadth <= self.analysis_rules['breadth_levels']['oversold']:
            return {
                'status': '超卖',
                'description': '市场处于超卖状态，需要关注企稳信号',
                'alert_level': 'warning'
            }
        elif breadth >= self.analysis_rules['breadth_levels']['overbought']:
            return {
                'status': '超买',
                'description': '市场处于超买状态，需要注意回调风险',
                'alert_level': 'danger'
            }
        elif breadth >= self.analysis_rules['breadth_levels']['neutral']:
            return {
                'status': '偏强',
                'description': '市场处于强势状态，可以保持乐观',
                'alert_level': 'success'
            }
        else:
            return {
                'status': '中性',
                'description': '市场处于中性状态，建议观望',
                'alert_level': 'info'
            }
    
    def _analyze_trend(self, recent_breadth: pd.DataFrame) -> dict:
        """分析近期趋势"""
        trend = recent_breadth['breadth'].diff().mean()
        
        if trend > 1:
            return {
                'direction': '上升',
                'strength': '强',
                'description': '市场宽度呈现明显上升趋势'
            }
        elif trend > 0:
            return {
                'direction': '上升',
                'strength': '弱',
                'description': '市场宽度呈现缓慢上升趋势'
            }
        elif trend < -1:
            return {
                'direction': '下降',
                'strength': '强',
                'description': '市场宽度呈现明显下降趋势'
            }
        else:
            return {
                'direction': '下降',
                'strength': '弱',
                'description': '市场宽度呈现缓慢下降趋势'
            }
    
    def _check_divergence(self, recent_breadth: pd.DataFrame, recent_index: pd.DataFrame) -> dict:
        """检查背离情况"""
        breadth_change = recent_breadth['breadth'].pct_change().sum()
        index_change = recent_index['Close'].pct_change().sum()
        
        if abs(breadth_change - index_change) > self.analysis_rules['divergence_threshold']:
            if breadth_change > index_change:
                return {
                    'exists': True,
                    'type': '正背离',
                    'description': '市场宽度强于大盘表现，可能预示上涨'
                }
            else:
                return {
                    'exists': True,
                    'type': '负背离',
                    'description': '市场宽度弱于大盘表现，需要警惕回调'
                }
        
        return {
            'exists': False,
            'type': '无背离',
            'description': '市场宽度与大盘走势基本一致'
        }
    
    def _generate_signals(self, current_breadth: float, recent_breadth: pd.DataFrame) -> list:
        """生成交易信号"""
        signals = []
        
        if current_breadth <= self.analysis_rules['breadth_levels']['extreme_oversold']:
            signals.append({
                'type': '买入',
                'strength': '强',
                'description': '市场极度超卖，可以考虑逢低买入'
            })
        
        if current_breadth >= self.analysis_rules['breadth_levels']['overbought']:
            signals.append({
                'type': '卖出',
                'strength': '强',
                'description': '市场超买，可以考虑适度减仓'
            })
        
        return signals
    
    def _assess_risk(self, recent_breadth: pd.DataFrame, recent_index: pd.DataFrame) -> dict:
        """评估市场风险水平"""
        volatility = recent_breadth['breadth'].std()
        
        if volatility > 15:
            return {
                'level': '高',
                'description': '市场波动剧烈，风险较大'
            }
        elif volatility > 10:
            return {
                'level': '中',
                'description': '市场波动适中，风险可控'
            }
        else:
            return {
                'level': '低',
                'description': '市场波动平稳，风险较小'
            }
    
    def _analyze_bullish_alignment(self, bullish_percentage: float) -> dict:
        """分析多头排列比例"""
        if bullish_percentage >= 50:
            return {
                'status': '强势',
                'description': '超过半数股票呈现多头排列，市场整体处于强势',
                'alert_level': 'success'
            }
        elif bullish_percentage >= 30:
            return {
                'status': '偏强',
                'description': '较多股票呈现多头排列，市场偏向乐观',
                'alert_level': 'info'
            }
        elif bullish_percentage >= 15:
            return {
                'status': '中性',
                'description': '部分股票呈现多头排列，市场趋势不明确',
                'alert_level': 'warning'
            }
        else:
            return {
                'status': '弱势',
                'description': '很少股票呈现多头排列，市场可能处于弱势',
                'alert_level': 'danger'
            }
