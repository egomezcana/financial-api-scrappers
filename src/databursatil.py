import requests,json
from datetime import date, timedelta

class DataBursatil:
    """Clase muy simple para contactar con la API de DataBursatil"""
    token = None

    def __init__ (self, user_token):
        self.token = user_token

    @staticmethod
    def _mondays_between (init, end):
        """Pequeña función que genera una lista con todos los viernes entre las
        fechas que se introducen"""

        days_since_monday_init = init.weekday()
        init_monday = init - timedelta(days=days_since_monday_init)

        days_since_monday_end = end.weekday()
        if days_since_monday_end >= 4:
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
        current_week   = [current_monday + timedelta(days=i) for i in range(5)]

        return current_week

    def last_price (self, ticker, series="*"):
        """Función para consulta el último precio registrado en la plataforma y
        devolverlo como float"""
        URL = f"https://api.databursatil.com/v2/cotizaciones?token={self.token}&emisora_serie={ticker+series}&concepto=u&bolsa=bmv"
        req = requests.get(URL)
        response = json.loads(req.text)
        return float(response[ticker+series]["bmv"]["u"])

    def price_history (self, init, end, ticker, series="*"):
        """Función para consultar los históricos y devolver un diccionario con
        las fechas de interés"""
        if end == init:
            return {}
        URL  =  "https://api.databursatil.com/v2/historicos?"
        URL += f"token={self.token}&inicio={init.strftime('%Y-%m-%d')}&final={end.strftime('%Y-%m-%d')}"
        URL += f"&emisora_serie={ticker+series}"
        req  = requests.get(URL)
        response = json.loads(req.text)
        return {date.fromisoformat(date_str) : float(response[date_str][0]) for date_str in response.keys()}

    def weekly_mean_price_history (self, init, end, ticker, series="*"):
        """Función para consultar los históricos y devolver un diccionario
        únicamente con las fechas de interés"""
        mondays = self._mondays_between(init,end)

        if len(mondays) < 0 :
            return {}

        prices = self.price_history(init=mondays[0], end=mondays[-1], ticker=ticker, series=series)

        week_mean_prices = {}
        for monday in mondays:
            week_dates  = self._week_list(monday)
            week_prices = [ prices[day] for day in week_dates if day in prices and prices[day] != 0.0 ]
            mean_price = sum(week_prices)/len(week_prices) if len(week_prices) != 0 else 0.0
            week_mean_prices[monday] = mean_price

        return week_mean_prices
