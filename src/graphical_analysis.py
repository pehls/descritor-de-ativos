from src.logger import logger

from src.directory_utilities import get_json_from_file

from src.indicators import Indicators
from datetime import datetime, timedelta
import logging
import pandas as pd
import numpy as np



import plotly.graph_objects as go

from plotly.subplots import make_subplots

import plotly.io as pio
pio.renderers.default = "notebook_connected"

class analyzer:
    _signals = {
        "Buy": "Sinal de Compra (Long)",
        "Sell": "Sinal de Venda (Short)"
    }

    def __init__(self, coin_pair):
        self.coin_pair = coin_pair
    def _test_EMASMA(self, tupla, ma_crossovers):
        ma_crossovers = self._filter_df_bydatetime(tupla, ma_crossovers)
        if (len(ma_crossovers)>0):
            last_cross = ma_crossovers.tail(1).reset_index().iloc[0]
            diff_text = self._get_diff_days(tupla, ma_crossovers)
            return_text = ""
            if (tupla.EMA_9 > tupla.SMA_21):
                return_text = return_text + (f"- Possui média móvel exponencial de 9 períodos (${round(tupla.EMA_9,2)}) acima da média simples de 21 períodos (${round(tupla.SMA_21,2)}), "
                )
            if (tupla.EMA_9 < tupla.SMA_21):
                return_text = return_text + (f"- Possui média móvel exponencial de 9 períodos (${round(tupla.EMA_9,2)}) abaixo da média simples de 21 períodos (${round(tupla.SMA_21,2)}), "
                )
            if (tupla.EMA_9 == tupla.SMA_21):
                return_text = return_text + f", com média móvel exponencial de 9 períodos (${round(tupla.EMA_9,2)}) igual à média simples de 21 períodos (${round(tupla.SMA_21,2)})"
            if (len(ma_crossovers) > 0):
                return_text = return_text + f" O último cruzamento aconteceu {diff_text} atrás, representando um {self._signals.get(last_cross.Signal)}."
            return return_text + "\n"
        return ""

    def _test_rsi(self, tupla):
        if (tupla.RSI < 30):
            return f"- Possui o oscilador RSI Sobrevendido ({round(tupla.RSI,2)})\n"
        if (tupla.RSI > 70):
            return f"- Possui o oscilador RSI Sobrecomprado ({round(tupla.RSI,2)})\n"
        return f"- Possui o oscilador RSI em uma área neutra ({round(tupla.RSI,2)})\n"

    def _test_support(self, tupla, suportes):
        """
        """
        suportes = self._filter_df_bydatetime(tupla, suportes)
        if (len(suportes)>0):
            actual_value = round(tupla.Close,2)
            actual_low_value = round(tupla.Low,2)
            last_sup_value = round(suportes.tail(1).reset_index().iloc[0].Low,2)
            dif_percentual = round((actual_value - last_sup_value) / last_sup_value * 100,2)
            if (dif_percentual > 0 and actual_low_value > last_sup_value):
                return (
                    "- O último suporte foi em "+suportes.tail(1).reset_index().iloc[0].Datetime.strftime('%d/%m/%Y, ás %H:%M:%S')
                    +f" e o fechamento atual (${actual_value}) é {dif_percentual}% maior que o último suporte (${last_sup_value})\n"
                    )
            if (last_sup_value == actual_low_value):
                return (
                    "- Está posicionados no suporte (${actual_low_value}) pelo valor mínimo de 21 pontos, "
                    +f" e o fechamento atual (${actual_value}) é {dif_percentual}% maior que o último suporte (${last_sup_value})\n"
                )
            return (
                f"- O último suporte foi em "+suportes.tail(1).reset_index().iloc[0].Datetime.strftime('%d/%m/%Y, ás %H:%M:%S')
                + f" com o valor de (${last_sup_value})\n"
                )
        return ""

    def _test_resistance(self, tupla, resistencias):
        """
        se temos um fundo e um topo (candle abaixo e um novo candle acima do anterior, mas abaixo da tendencia), há um cancelamento da tendência
        """
        resistencias = self._filter_df_bydatetime(tupla, resistencias)
        if (len(resistencias)>0):
            actual_value = round(tupla.Close,2)
            actual_high_value = round(tupla.Low,2)
            last_res_value = round(resistencias.tail(1).reset_index().iloc[0].High,2)
            dif_percentual = round((actual_value - last_res_value) / last_res_value * 100,2)
            if (dif_percentual > 0 and actual_high_value > last_res_value):
                return (
                    "- A última resistência foi em "+resistencias.tail(1).reset_index().iloc[0].Datetime.strftime('%d/%m/%Y, ás %H:%M:%S')
                    +f" e o fechamento atual (${actual_value}) é {dif_percentual}% maior que a última resistência (${last_res_value})\n"
                    )
            if (dif_percentual < 0 and actual_high_value < last_res_value):
                return (
                    "- A última resistência foi em "+resistencias.tail(1).reset_index().iloc[0].Datetime.strftime('%d/%m/%Y, ás %H:%M:%S')
                    +f" e o fechamento atual (${actual_value}) é {dif_percentual * -1}% menor que a última resistência (${last_res_value})\n"
                    )
            if (last_res_value == actual_high_value):
                return (
                    "- Está posicionados na resistência (${actual_high_value}) pelo valor mínimo de 21 pontos, "
                    +f" e o fechamento atual (${actual_value}) é {dif_percentual}% maior que a resistência (${last_res_value})\n"
                )
            return (
                f"- A última resistência foi em "+resistencias.tail(1).reset_index().iloc[0].Datetime.strftime('%d/%m/%Y, ás %H:%M:%S')
                + f" com o valor de (${last_res_value})\n"
                )
            return f", a última resistência foi em "+resistencias.tail(1).reset_index().iloc[0].Datetime.strftime('%d/%m/%Y, ás %H:%M:%S') + "\n"
        return ""

    def _test_macd(self, tupla, crossovers):
        crossovers = self._filter_df_bydatetime(tupla, crossovers)
        if (len(crossovers)>0):
            last_macd_crossover = crossovers.tail(1).reset_index().iloc[0]
            diff_text = self._get_diff_days(tupla, crossovers)
            if (len(crossovers) > 0):
                if (last_macd_crossover.Datetime != tupla.Datetime):
                    return (
                        f"- O último cruzamento do MACD foi em {last_macd_crossover.Datetime.strftime('%d/%m/%Y, ás %H:%M:%S')}"
                        + f", {diff_text} atrás, e foi um {self._signals.get(last_macd_crossover.Signal)}\n"
                    )
                else:
                    return (
                        f"- Estamos posicionados em um cruzamento do MACD, posicionado como um {self._signals.get(last_macd_crossover.Signal)}\n"
                    )
            return ""
        return ""

    def _test_candles(self, tupla, hammers):
        hammers = self._filter_df_bydatetime(tupla, hammers)
        if (len(hammers) > 0):
            last_hammer = hammers.tail(1).reset_index().iloc[0]
            diff_text = self._get_diff_days(tupla, hammers)
            if (last_hammer.Datetime != tupla.Datetime):
                return (
                    f"- O último hammer foi em {last_hammer.Datetime.strftime('%d/%m/%Y, ás %H:%M:%S')}"
                    + f", {diff_text} atrás\n"
                )
            else:
                return (
                    f"- Estamos posicionados em um hammer\n"
                )
        return ""

    def _get_diff_days(self, tupla, df):
        df = self._filter_df_bydatetime(tupla, df)
        difference = (tupla.Datetime - df.tail(1).reset_index().iloc[0].Datetime)
        diff_text = f"{difference.days} dias"
        if (difference.seconds > 0):
            diff_text = diff_text + f" e {difference.seconds // 3600} horas"
        return diff_text

    def _filter_df_bydatetime(self, tupla, df):
        return df[df.Datetime <= tupla.Datetime]

    def _test_HL_divergence(self, tupla, df):
        df = self._filter_df_bydatetime(tupla, df)
        if (tupla.divergencia_alta == 1):
            return f"- Foi detectada uma divergência de alta, com preços mínimos em d-2 (${round(df.iloc[0].Low,2)}) até d (${round(df.iloc[2].Low,2)}), e o histograma do MACD entre (${round(df.iloc[0].MACD_Histogram,2)}) e (${round(df.iloc[2].MACD_Histogram,2)}) na data da análise\n"
        if (tupla.divergencia_baixa == 1):
            return f"- Foi detectada uma divergência de baixa, com preços máximos em d-2 (${round(df.iloc[0].High,2)}) até d (${round(df.iloc[2].High,2)}), e o histograma do MACD entre (${round(df.iloc[0].MACD_Histogram,2)}) e (${round(df.iloc[2].MACD_Histogram,2)}) na data da análise\n"
        return ""

    def _test_bb(self, tupla, bb_crossovers):
        return_text = "- "
        bb_crossovers = self._filter_df_bydatetime(tupla, bb_crossovers)
        if (len(bb_crossovers)>0):
            last_cross = bb_crossovers.tail(1).reset_index().iloc[0]
            diff_text = self._get_diff_days(tupla, bb_crossovers)
            if (tupla.Upper_Band <= tupla.Close):
                return_text += f"Preços sobre-comprados segundo as Bandas de Bollinger, com banda superior em (${round(tupla.Upper_Band,2)}), indicando Venda (Short). "
            if (tupla.Lower_Band >= tupla.Close):
                return_text += f"Preços sobre-vendidos segundo as Bandas de Bollinger, com banda inferior em (${round(tupla.Lower_Band,2)}), indicando Compra (Long). "
            return_text += f"O preço de fechamento está em (${round(tupla.Close,2)}) e a média móvel simples (de 20 períodos, ou banda do meio das Bandas de Bollinger) em (${round(tupla.Middle_Band,2)})"
            return_text += f"\n- O último toque nas Bandas de Bollinger aconteceu {diff_text} atrás, onde o preço de fechamento era (${round(last_cross.Close,2)}), com {self._signals.get(last_cross.Signal)} e bandas sup. em (${round(last_cross.Upper_Band,2)}) e inf. (${round(last_cross.Lower_Band,2)})"
            return return_text + "\n"
        return ""

    def _test_OBV(self, tupla, df):
        df = self._filter_df_bydatetime(tupla, df)
        df_to_obv = df.tail(4).reset_index()
        text_obv = ""
        for i, row in df_to_obv.iterrows():
            if (i > 3):
                obv_m3 = df.loc[i-3, ['OBV']][0]
                obv_m2 = df.loc[i-2, ['OBV']][0]
                obv_m1 = df.loc[i-1, ['OBV']][0]
                obv_m0 = df.loc[i, ['OBV']][0]
                c_m3 = df.loc[i-3, ['Close']][0]
                c_m2 = df.loc[i-2, ['Close']][0]
                c_m1 = df.loc[i-1, ['Close']][0]
                c_m0 = df.loc[i, ['Close']][0]
                if ((obv_m2) > (obv_m1) > (obv_m0)):
                    text_obv += (f'- O OBV ({obv_m2} / {obv_m1} / {obv_m0}) está em queda; ')
                if ((obv_m2) < (obv_m1) < (obv_m0)):
                    text_obv += (f'- O obv ({obv_m2} / {obv_m1} / {obv_m0}) está em alta; ')
                if ((c_m2) < (c_m1) < (c_m0)):
                    text_obv += (f"- Os Preços de Fechamento (${round(c_m2,2)} / ${round(c_m1, 2)} / ${round(c_m0,2)}) estão em alta; ")
                if ((c_m2) > (c_m1) > (c_m0)):
                    text_obv += (f"- Os Preços de Fechamento (${round(c_m2,2)} / ${round(c_m1, 2)} / ${round(c_m0,2)}) estão em queda; ")
                if (obv_m3 > obv_m2 > obv_m1 < obv_m0):
                    text_obv += (f"- O OBV ({obv_m3} / {obv_m2} / {obv_m1} / {obv_m0}) teve um rompimento de um movimento de alta no último dia analisado, emitindo um sinal de venda")
                if (obv_m3 < obv_m2 < obv_m1 > obv_m0):
                    text_obv += (f"- O OBV ({obv_m3} / {obv_m2} / {obv_m1} / {obv_m0}) teve um rompimento de um movimento de queda no último dia analisado, emitindo um sinal de compra")
        return text_obv + "\n"

    def translate_graph_fromdf(self, df, crossovers, ma_crossovers, bb_crossovers, hammers, suportes, resistencias, high_trend, low_trend, close_trend):
        for i, row in df.iterrows():
            last_datetime_information = df.iloc[i]
            pre_msg = f"O ativo {self.coin_pair}, na data de {last_datetime_information.Datetime.strftime('%d/%m/%Y, ás %H:%M:%S')}: \n"
            msg = (
                pre_msg 
                + self._test_EMASMA(last_datetime_information, ma_crossovers) 
                + self._test_rsi(last_datetime_information) 
                + self._test_support(last_datetime_information, suportes) 
                + self._test_resistance(last_datetime_information, resistencias) 
                + self._test_macd(last_datetime_information, crossovers)
                + self._test_bb(last_datetime_information, bb_crossovers)
                + self._test_candles(last_datetime_information, hammers) 
                + self._test_OBV(last_datetime_information, df)
                + self._test_HL_divergence(last_datetime_information, df)
            )
            df.loc[i, ['translated_message']] = msg
        return df

    def translate_graph_fromtuple(self, last_datetime_information, df, crossovers, ma_crossovers, bb_crossovers, hammers, suportes, resistencias, high_trend, low_trend, close_trend):
        pre_msg = f"O ativo {self.coin_pair}, na data de {last_datetime_information.Datetime.strftime('%d/%m/%Y, ás %H:%M:%S')}: \n"
        msg = (
            pre_msg 
            + self._test_EMASMA(last_datetime_information, ma_crossovers) 
            + self._test_rsi(last_datetime_information) 
            + self._test_support(last_datetime_information, suportes) 
            + self._test_resistance(last_datetime_information, resistencias) 
            + self._test_macd(last_datetime_information, crossovers)
            + self._test_bb(last_datetime_information, bb_crossovers)
            + self._test_candles(last_datetime_information, hammers) 
            + self._test_OBV(last_datetime_information, df)
            + self._test_HL_divergence(last_datetime_information, df)
        )
        last_datetime_information['translated_message'] = msg
        return last_datetime_information


def generate_graph(df, crossovers):
    INCREASING_COLOR = '#3D9970'
    DECREASING_COLOR = '#FF4136'

    df.index = df.Datetime

    fig = make_subplots(rows=5, cols=1)
    fig = fig.update_layout(title='Candle Plot w/ Plotly', title_yanchor='middle')
    fig = fig.add_trace(go.Candlestick(x=df.index,
                                       open=df['Open'],
                                       high=df['High'],
                                       low=df['Low'],
                                       close=df['Close']),
                        row=1, col=1
                       )

    fig.add_trace(
        go.Scatter(x=df.index, y=df['Upper_Band'],#
               name='Upper BB',  
               marker=dict(color=['black'])
        ),
        row=1, col=1
    )
    fig.add_trace(
        go.Scatter(x=df.index, y=df['Lower_Band'],#
               name='Lower BB', 
               marker=dict(color=['black'])
        ),
        row=1, col=1
    )
    fig = fig.update_layout(xaxis_rangeslider_visible=False, xaxis={'visible':False, 'showticklabels':False})
    # volume bar chart

    colors = []

    for i in range(len(df.Close)):
        if i != 0:
            if df.Close[i] > df.Close[i-1]:
                colors.append(INCREASING_COLOR)
            else:
                colors.append(DECREASING_COLOR)
        else:
            colors.append(DECREASING_COLOR)


    # add vol to graph
    fig.add_trace(
        go.Bar(x=df.index, y=df.Volume,
               marker=dict(color=colors),#
               name='Volume'),
        row=2, col=1

    )
    # OBV
    fig.add_trace(
        go.Scatter(x=df.index, y=df.OBV,
               marker=dict(color=colors),#
               name='OBV'),
        row=2, col=1

    )
    
    fig = fig.update_layout(xaxis={'visible':False, 'showticklabels':False}, yaxis={'visible':False, 'showticklabels':False})
    
    colors_macd = []

    for i in range(len(df.MACD_Histogram)):
        if i != 0:
            if df.MACD_Histogram[i] >= 0:
                colors_macd.append(INCREASING_COLOR)
            else:
                colors_macd.append(DECREASING_COLOR)
        else:
            colors_macd.append(DECREASING_COLOR)
    # add MACD HIST to graph
    fig.add_trace(
        go.Bar(x=df.index, y=df.MACD_Histogram,
               marker=dict(color=colors_macd),#
               name='MACD Histogram'),
        row=3, col=1

    )
    # add 12-EMA
    fig.add_trace(
        go.Scatter(x=df.index, y=df.EMA_9,#
               name='EMA_9'),
        row=4, col=1

    )
    # add 26-EMA
    fig.add_trace(
        go.Scatter(x=df.index, y=df.SMA_21,#
               name='SMA_21'),
        row=4, col=1

    )
    
    # add buy&sell markers
    #fig.add_trace(
    #    go.Scatter(mode="markers",x=crossovers.Datetime, y=crossovers.EMA_Short, 
    #           marker_symbol = [(_sygnal_marker.get(x)) for x in crossovers['Signal']],
    #           marker_color = [(_sygnal_color.get(x)) for x in crossovers['Signal']],
    #           marker_size=15, name="Buy/Sell"
    #          ),
    #    row=3, col=1
    #)
    fig = fig.update_layout(xaxis={'visible':False, 'showticklabels':False}, yaxis={'visible':False, 'showticklabels':False})
    
    # add MACD markers
    fig.add_trace(
        go.Scatter(x=df.Datetime, y=df.MACD, 
               marker=dict(color="black"), name='MACD'
              ),
        row=5, col=1
    )
    fig.add_trace(
        go.Scatter(x=df.Datetime, y=df.MACD_Signal_line, 
               marker=dict(color="blue"), name='MACD Signal'
              ),
        row=5, col=1
    )
    fig = fig.update_layout(yaxis={'visible':False, 'showticklabels':False})
    fig = fig.update_layout(xaxis_rangeslider_visible=False)
    fig.show()