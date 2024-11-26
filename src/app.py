import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime
import os
import sys

# 添加项目根目录到 Python 路径
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)  # 使用 insert 确保我们的路径在最前面

# 使用从根目录开始的导入路径
from src.data.data_fetcher import DataFetcher
from src.analysis.market_breadth import MarketBreadth
from src.analysis.market_analysis import MarketAnalysis

def create_gauge_chart(percentage: float, title: str) -> go.Figure:
    """创建仪表盘图表"""
    fig = go.Figure(go.Indicator(
        mode = "gauge+number",
        value = percentage,
        title = {'text': title},
        domain = {'x': [0, 1], 'y': [0, 1]},
        gauge = {
            'axis': {'range': [0, 100]},
            'bar': {'color': "darkblue"},
            'steps': [
                {'range': [0, 20], 'color': "red"},
                {'range': [20, 40], 'color': "orange"},
                {'range': [40, 60], 'color': "yellow"},
                {'range': [60, 80], 'color': "lightgreen"},
                {'range': [80, 100], 'color': "green"}
            ]
        }
    ))
    return fig

def create_breadth_chart(breadth_df: pd.DataFrame, index_df: pd.DataFrame, ma_period: int) -> go.Figure:
    """创建市场宽度和标普500对比图表"""
    fig = go.Figure()
    
    # 添加市场宽度线
    fig.add_trace(go.Scatter(
        x=breadth_df['date'],
        y=breadth_df['breadth'],
        name='市场宽度',
        line=dict(color='#1f77b4', width=2),
        yaxis='y'
    ))
    
    # 处理标普500数据
    if not index_df.empty and not breadth_df.empty:
        # 将index_df的索引转换为datetime
        index_df.index = pd.to_datetime(index_df.index)
        
        # 获取与市场宽度数据相匹配的时间范围
        start_date = breadth_df['date'].min()
        end_date = breadth_df['date'].max()
        
        # 筛选时间范围内的标普500数据
        mask = (index_df.index >= start_date) & (index_df.index <= end_date)
        filtered_index_df = index_df[mask].copy()
        
        if not filtered_index_df.empty:
            # 计算相对于起始点的百分比变化
            start_price = filtered_index_df['Close'].iloc[0]
            filtered_index_df['relative_change'] = ((filtered_index_df['Close'] - start_price) / start_price) * 100
            
            # 添加标普500曲线
            fig.add_trace(go.Scatter(
                x=filtered_index_df.index,
                y=filtered_index_df['relative_change'],
                name='标普500',
                line=dict(color='#ff7f0e', width=2),
                yaxis='y2'
            ))
            
            # 计算y2轴的范围
            y2_min = filtered_index_df['relative_change'].min()
            y2_max = filtered_index_df['relative_change'].max()
            # 增加padding使图表更美观
            padding = (y2_max - y2_min) * 0.2
            y2_range = [y2_min - padding, y2_max + padding]
    
    # 添加超买超卖区域
    fig.add_hrect(
        y0=80, y1=100,
        fillcolor="rgba(255, 0, 0, 0.1)",
        layer="below", line_width=0,
        name="超买区"
    )
    
    fig.add_hrect(
        y0=0, y1=20,
        fillcolor="rgba(0, 255, 0, 0.1)",
        layer="below", line_width=0,
        name="超卖区"
    )
    
    # 更新布局
    fig.update_layout(
        title=dict(
            text=f"市场宽度与标普500走势对比 ({ma_period}日均线)",
            font=dict(size=24, color="#262730")
        ),
        plot_bgcolor='white',
        paper_bgcolor='white',
        xaxis=dict(
            title="日期",
            gridcolor='#E5E5E5',
            showgrid=True,
            rangeslider=dict(visible=True),
            rangeselector=dict(
                buttons=list([
                    dict(count=1, label="1月", step="month", stepmode="backward"),
                    dict(count=6, label="6月", step="month", stepmode="backward"),
                    dict(count=1, label="1年", step="year", stepmode="backward"),
                    dict(count=2, label="2年", step="year", stepmode="backward"),
                    dict(step="all", label="全部")
                ])
            )
        ),
        yaxis=dict(
            title="市场宽度 (%)",
            range=[0, 100],
            side="left",
            gridcolor='#E5E5E5',
            showgrid=True,
            tickfont=dict(color="#1f77b4")
        ),
        yaxis2=dict(
            title="标普500涨跌幅 (%)",
            side="right",
            overlaying="y",
            showgrid=False,
            tickfont=dict(color="#ff7f0e"),
            range=y2_range if 'y2_range' in locals() else None
        ),
        hovermode='x unified',
        showlegend=True,
        legend=dict(
            bgcolor='rgba(255,255,255,0.9)',
            bordercolor='#E5E5E5',
            borderwidth=1,
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01
        )
    )
    
    # 添加对 rangeslider 的更新
    fig.update_layout(
        xaxis_rangeslider_visible=True,
        xaxis_rangeslider_thickness=0.05
    )
    
    return fig

def display_market_analysis(analysis_results: dict):
    """展示市场分析结果"""
    st.subheader("市场分析报告")
    
    # 市场状态
    status = analysis_results['market_status']
    st.markdown(f"**市场状态**: {status['status']}")
    st.markdown(f"_{status['description']}_")
    
    # 趋势分析
    trend = analysis_results['trend']
    st.markdown(f"**趋势分析**: {trend['description']}")
    
    # 背离分析
    divergence = analysis_results['divergence']
    if divergence['exists']:
        st.warning(f"**背离警告**: {divergence['description']}")
    
    # 交易信号
    if analysis_results['signals']:
        st.subheader("交易信号")
        for signal in analysis_results['signals']:
            if signal['type'] == '买入':
                st.success(f"**{signal['type']}信号**: {signal['description']}")
            else:
                st.error(f"**{signal['type']}信号**: {signal['description']}")
    
    # 风险评估
    risk = analysis_results['risk_level']
    st.markdown(f"**风险水平**: {risk['description']}")

def main():
    # 设置页面配置
    st.set_page_config(
        page_title="市场宽度分析器",
        page_icon="📈",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # 初始化状态变量
    if 'breadth_value' not in st.session_state:
        st.session_state.breadth_value = 0
    if 'sp500_value' not in st.session_state:
        st.session_state.sp500_value = 0
    if 'change_value' not in st.session_state:
        st.session_state.change_value = 0
    
    # 使用列布局优化界面
    col1, col2, col3 = st.columns([1, 3, 1])
    
    with col1:
        st.sidebar.header("参数设置")
        
        # 添加时间周期选择
        time_period = st.sidebar.selectbox(
            "分析时间周期",
            [
                ("1个月", 30),
                ("3个月", 90),
                ("6个月", 180),
                ("1年", 365),
                ("2年", 730)
            ],
            format_func=lambda x: x[0]
        )
        
        # 修改均线周期选择，添加完全多头排列选项
        ma_period = st.sidebar.selectbox(
            "均线周期",
            [
                ("21日均线", 21),
                ("63日均线", 63),
                ("127日均线", 127),
                ("完全多头排列", "bullish")  # 添加新选项
            ],
            format_func=lambda x: x[0]
        )
        ma_period = ma_period[1]  # 获取选项的值
        
        sample_size = st.sidebar.slider(
            "分析样本数量",
            min_value=10,
            max_value=500,
            value=30,
            step=10
        )
    
    with col2:
        st.title("市场宽度分析器 📈")
        
        # 主要内容区域
        if st.button("开始分析", key="analyze_button"):
            try:
                data_fetcher = DataFetcher()
                market_breadth = MarketBreadth(data_fetcher)
                symbols = data_fetcher.get_sp500_components()[:sample_size]
                lookback_days = time_period[1]
                
                # 获取指数数据
                index_df = market_breadth.get_index_data(lookback_days)
                
                if ma_period == "bullish":
                    # 使用新的历史多头排列计算方法
                    breadth_df = market_breadth.calculate_historical_bullish_alignment(
                        symbols, 
                        lookback_days
                    )
                    if not breadth_df.empty:
                        st.session_state.breadth_value = breadth_df['breadth'].iloc[-1]
                        
                        # 显示多头排列的详细信息
                        st.subheader("多头排列分析")
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            latest_data = breadth_df.iloc[-1]
                            st.markdown(f"""
                            - 多头排列股票数量: {int(latest_data['bullish_count'])}只
                            - 有效样本数量: {int(latest_data['valid_stocks'])}只
                            - 多头排列比例: {latest_data['breadth']:.1f}%
                            """)
                        
                        with col2:
                            st.markdown("""
                            **多头排列说明**:
                            - 完全多头排列指股价位于所有均线之上
                            - 且21日均线>63日均线>127日均线
                            - 表示个股处于强势上涨趋势中
                            """)
                else:
                    # 原有的市场宽度计算逻辑
                    breadth_df = market_breadth.calculate_historical_breadth(
                        symbols, 
                        ma_period,
                        lookback_days=lookback_days
                    )
                    if not breadth_df.empty:
                        st.session_state.breadth_value = breadth_df['breadth'].iloc[-1]
                
                # 更新指数相关的状态
                if not index_df.empty:
                    st.session_state.sp500_value = index_df['Close'].iloc[-1]
                    start_price = index_df['Close'].iloc[0]
                    end_price = index_df['Close'].iloc[-1]
                    st.session_state.change_value = ((end_price - start_price) / start_price) * 100
                
                # 创建图表
                fig = create_breadth_chart(breadth_df, index_df, ma_period)
                st.plotly_chart(fig, use_container_width=True)
                
                # 添加市场分析
                market_analyzer = MarketAnalysis()
                analysis_results = market_analyzer.analyze_market_condition(
                    breadth_df, 
                    index_df,
                    {'bullish_percentage': st.session_state.breadth_value} if ma_period == "bullish" else None
                )
                
                # 显示分析结果
                display_market_analysis(analysis_results)
                
            except Exception as e:
                st.error(str(e))  # 修改错误显示方式
                logging.error(f"分析过程中出现错误: {str(e)}")  # 添加日志记录
        
        # 使用网格布局显示指标
        metrics_container = st.container()
        with metrics_container:
            m1, m2, m3, m4 = st.columns(4)
            with m1:
                st.metric("当前市场宽度", f"{st.session_state.breadth_value:.1f}%")
            with m2:
                st.metric("标普500点位", f"{st.session_state.sp500_value:.2f}")
            with m3:
                st.metric("涨跌幅", f"{st.session_state.change_value:.2f}%")
            with m4:
                st.metric("样本数量", f"{sample_size}只")
    
    with col3:
        st.markdown("### 市场状态")
        st.info(get_market_status(st.session_state.breadth_value))

def get_market_status(breadth: float) -> str:
    """根据市场宽度返回市场状态描述"""
    if breadth >= 80:
        return "极度超买"
    elif breadth >= 60:
        return "超买"
    elif breadth <= 20:
        return "极度超卖"
    elif breadth <= 40:
        return "超卖"
    return "中性"

if __name__ == "__main__":
    main()
