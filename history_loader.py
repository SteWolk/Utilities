import plotly.graph_objects as go

from utilities import load_existing_history, data_to_dataframe
import sys

exchange = sys.argv[1] if len(sys.argv) > 1 else 'bybit'
symbol=  sys.argv[2] if len(sys.argv) > 1 else 'BTCUSD'
timeframe= sys.argv[3] if len(sys.argv) > 1 else '4h'

data, nmbs = load_existing_history(exchange, symbol)
bars = data_to_dataframe(data, exchange = exchange)

# Resample to timeframe intervals
ohlc = {
    'open': 'first',
    'high': 'max',
    'low': 'min',
    'close': 'last',
    'volume': 'sum'
}
bars = bars.resample(timeframe).agg(ohlc)

# Convert the index back to a column
bars.reset_index(inplace=True)

fig = go.Figure(data=[go.Candlestick(x=bars['time'],
                open=bars['open'],
                high=bars['high'],
                low=bars['low'],
                close=bars['close'])])

fig.update_layout(autosize=True,dragmode='zoom',xaxis_rangeslider_visible=False)

fig.show()