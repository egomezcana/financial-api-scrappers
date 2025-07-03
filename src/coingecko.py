import requests,json
from datetime import date, datetime, time, timedelta

class CoinGecko:
    """Clase muy simple para contactar con la API de CoinGecko"""
    token = None

    def __init__ (self, user_token=None):
        self.token = user_token

    @staticmethod
    def _mondays_between (init, end):
        """Pequeña función que genera una lista con todos los lunes entre las
        fechas que se introducen"""

        days_since_monday_init = init.weekday()
        init_monday = init - timedelta(days=days_since_monday_init)

        days_since_monday_end = end.weekday()
        if days_since_monday_end >= 6:
            end_monday = end - timedelta(days=days_since_monday_end)
        else:
            end_monday = end - timedelta(days=days_since_monday_end + 7)

        weeks = (end_monday - init_monday).days // 7

        if weeks < 0:
            return []

        return [init_monday + timedelta(weeks=i) for i in range(weeks + 1)]

    @staticmethod
    def _week_list (current_date):
        """Pequeña función que genera una lista con todos los elementos de la
        semana de la fecha que se proporciona"""

        current_monday = current_date - timedelta(days=current_date.weekday())
        current_week   = [current_monday + timedelta(days=i) for i in range(7)]

        return current_week

    def last_price (self, coin_name):
        """Función para consulta el último precio registrado en la plataforma y
        devolverlo como float"""
        URL = f"https://api.coingecko.com/api/v3/simple/price?ids={coin_name}&vs_currencies=mxn"
        req = requests.get(URL)
        response = json.loads(req.text)
        return float(response[coin_name]["mxn"])

    def price_history (self, init, end, coin_name):
        """Función para consultar los históricos y devolver un diccionario con
        las fechas de interés"""
        if end == init:
            return {}
        init_timestamp = int(datetime.combine(init, time.min).timestamp())
        end_timestamp = int(datetime.combine(end, time.min).timestamp())
        URL  = f"https://api.coingecko.com/api/v3/coins/{coin_name}/market_chart/range?"
        URL += f"vs_currency=mxn&from={init_timestamp}&to={end_timestamp}&precision=2"
        req  = requests.get(URL)
        response = json.loads(req.text)
        return {datetime.utcfromtimestamp(stamp/1000).date() : float(price)  for stamp, price in response["prices"]}

    def weekly_mean_price_history (self, init, end, coin_name):
        """Función para consultar los históricos y devolver un diccionario
        únicamente con las fechas de interés"""
        mondays = self._mondays_between(init,end)

        if len(mondays) < 0 :
            return {}

        prices = self.price_history(init=mondays[0], end=mondays[-1], coin_name=coin_name)

        week_mean_prices = {}
        for monday in mondays:
            week_dates  = self._week_list(monday)
            week_prices = [ prices[day] for day in week_dates if day in prices and prices[day] != 0.0 ]
            mean_price = sum(week_prices)/len(week_prices) if len(week_prices) != 0 else 0.0
            week_mean_prices[monday] = mean_price

        return week_mean_prices
