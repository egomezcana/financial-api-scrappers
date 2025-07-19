import sqlite3
from datetime import date, datetime, time

class FinancialDB:
    """Clase sencilla para el mantenimiento de los datos extraídos por los
    scrappers"""
    db_path = None

    def __init__ (self, filepath):
        """Constructor que define el nombre del archivo de la base de datos"""
    
        # Define la localización de la base de datos cuando se requiera realizar una
        # conexión
        self.db_path = filepath

    def _execute (self, calling_function):
        """Una evoltura para ~execute~ en SQLite. No se requiere toda la potencia de
        la librería al ser consultas muy dirigidas y la envoltura atrapa los errores
        y devuelve el resultado de la consulta para su manipulación posterior"""
    
        try:
            # Envuelve la posibilidad de fallo en la conexión a base de datos
            conn = sqlite3.connect(self.db_path)
    
            # Genera un cursor y usa la función para indicar la ejecución que se
            # desea a través de usar el cursor como parámetro
            cursor = conn.cursor()
            calling_function(cursor)
    
            # Guarda los posiles cambios realizados a la base de datos
            conn.commit()
    
            # Extrae la información que coleccionó el cursor de la ejecución
            return { 'fetched' : cursor.fetchall(),
                     'rowcount': cursor.rowcount,
                     'lastrowid': cursor.lastrowid }
    
        except sqlite3.Error as error:
            # Atrapa cualquier error en la ejecución de la base de datos y lo
            # devuelve para informar cuál fue el problema
            return error
    
        finally:
            # Una vez que retorna la función, se garantiza que la conexión se cierra
            # adecuadamente
            if conn:
                conn.close()

    def _execute_query (self, query_str, parameters=()):
        """Una evoltura para ~execute_many~ en SQLite para manejar los posibles
        problemas de manera externa"""
    
        # Indica cómo debe llamarse a execute usando el cursor cuando esté
        # disponible al conectarse a la base de datos
        return self._execute(lambda cur: cur.execute(query_str, parameters))

    def _execute_many (self, query_str, parameters):
        """Una evoltura para ~execute_many~ en SQLite para manejar los posibles
        problemas de manera externa"""
    
        # Indica cómo debe llamarse a execute_many usando el cursor cuando esté
        # disponible al conectarse a la base de datos
        return self._execute(lambda cur: cur.executemany(query_str, parameters))

    def _symbols_ids (self):
        """La función cumple una función auxiliar, hace una consulta de los IDs
        correspondientes con los productos registrados. El uso principal se da
        cuando deben insertarse datos nuevos en las tablas que compras y precios"""
    
        # Define una query para traer los IDs requeridos
        SQL_QUERY = "SELECT id, symbol, serie FROM products"
    
        # Ejecuta la query en la base de datos
        result = self._execute_query(SQL_QUERY)
    
        # Genera un diccionario para devolver el ID
        return { (symbol, serie) : db_id for db_id, symbol, serie in result["fetched"]}

    @staticmethod
    def _utc2date(utc_timestamp):
        return datetime.utcfromtimestamp(utc_timestamp).date()

    @staticmethod
    def _date2utc(given_date):
        return int(datetime.combine(given_date, time.min).timestamp())

    def consult_scrap_date (self, symbols_list):
        """Dada una lista que describe parejas símbolo+serie, devuelve un
        diccionario usando esa misma pareja como clave y la información que se
        requiere para hacer una consulta con el scrapper lo cual consiste en la
        última fecha guardada y el origen del símbolo"""
    
        # Define la instrucción requerida en la consulta
        placeholders = ','.join(['?']*len(symbols_list))
        SQL_QUERY = f"""SELECT products.symbol, products.serie, MAX(prices.date) FROM prices
        INNER JOIN products ON products.id = prices.symbol
        WHERE prices.symbol IN ({placeholders}) GROUP BY prices.symbol"""
    
        # Atrae el diccionario de IDs para símbolo+serie
        ids_dictionary = self._symbols_ids()
    
        # Genera la lista de IDs para ejecutar la operación
        data = [ids_dictionary[key_pair] for key_pair in symbols_list]
    
        # Ejecuta la consulta
        result = self._execute_query(SQL_QUERY, data)
    
        # Crea el diccionario con la última fecha guardada
        return { (symbol, serie) : self._utc2date(utc_timestamp)
                 for symbol, serie, utc_timestamp in result["fetched"]}

    def consult_last_value (self, symbols_list):
        """Dada una lista que describe parejas símbolo+serie, devuelve un
        diccionario usando esa misma pareja como clave y devuelve el último precio
        registrado y la fecha de consulta"""
    
        # Define la instrucción requerida en la consulta
        placeholders = ','.join(['?']*len(symbols_list))
        SQL_QUERY1 = f"""SELECT symbol, SUM(qty) AS total_qty
        FROM buys WHERE symbol IN ({placeholders}) GROUP BY symbol"""
    
        SQL_QUERY2 = f"""SELECT symbol, price, MAX(date) AS last_date
        FROM prices WHERE symbol IN ({placeholders}) GROUP BY symbol"""
    
        FULL_QUERY = f"""WITH total_buys AS ({SQL_QUERY1}), last_prices AS ({SQL_QUERY2})
        SELECT products.symbol, products.serie, total_buys.total_qty*last_prices.price, last_prices.last_date
        FROM total_buys
        JOIN last_prices ON total_buys.symbol=last_prices.symbol
        JOIN products ON products.id = total_buys.symbol"""
    
        # Atrae el diccionario de IDs para símbolo+serie
        ids_dictionary = self._symbols_ids()
    
        # Genera la lista de IDs para ejecutar la operación
        data = [ids_dictionary[key_pair] for key_pair in symbols_list]
    
        # Ejecuta la consulta y los placeholders deben acumularse
        result = self._execute_query(FULL_QUERY, data+data)
    
        # Crea el diccionario con la última fecha guardada y el valor económico
        return { (symbol, serie) : {"date" : self._utc2date(utc_timestamp), "value" : value}
                 for symbol, serie, value, utc_timestamp in result["fetched"]}

    def consult_section_value(self, exclude = []):
        """Consulta en la base de datos el valor acumulado de todos los activos en
        las diferentes secciones registradas en la table de productos a menos que
        sea excluida en la lista"""
    
        # Define la instrucción requerida en la consulta
        SQL_QUERY1 = f"""SELECT symbol, SUM(qty) AS total_qty
        FROM buys GROUP BY symbol"""
    
        SQL_QUERY2 = f"""SELECT symbol, price, MAX(date) AS last_date
        FROM prices GROUP BY symbol"""
    
        SQL_QUERY3 = f"""SELECT total_buys.symbol AS symbol, total_buys.total_qty*last_prices.price AS value
        FROM total_buys JOIN last_prices ON total_buys.symbol=last_prices.symbol"""
    
        FULL_QUERY = f"""WITH total_buys AS ({SQL_QUERY1}), last_prices AS ({SQL_QUERY2}), symbol_value AS ({SQL_QUERY3})
        SELECT products.secc, SUM(value) FROM symbol_value
        JOIN products ON products.id = symbol_value.symbol
        GROUP BY products.secc"""
    
        # Ejecuta la consulta y los placeholders deben acumularse
        result = self._execute_query(FULL_QUERY)
    
        # Crea el diccionario con la última fecha guardada y el valor económico
        return { section : round(last_value,2)
                 for section, last_value in result["fetched"] if section not in exclude}

    def consult_buys_timetable(self, symbols_list, init, end):
        """Consulta la lista de compras y devuelve un diccionario con las claves de
        los símbolos (symbol+serie) y loa valores son arreglos de parejas con la
        fecha de compra y el valor invertido hasta esa fecha"""
    
        # Define la instrucción requerida en la consulta
        placeholders = ','.join(['?']*len(symbols_list))
        SQL_QUERY = f"""SELECT products.symbol, products.serie, buys.date, buys.price FROM buys
        JOIN products ON products.id = buys.symbol
        WHERE buys.symbol IN ({placeholders}) ORDER BY buys.date"""
    
        # Atrae el diccionario de IDs para símbolo+serie
        ids_dictionary = self._symbols_ids()
    
        # Genera la información para generar la consulta
        data = [ids_dictionary[key_pair] for key_pair in symbols_list]
    
        # Ejecuta la consulta y los placeholders deben acumularse
        result = self._execute_query(SQL_QUERY, data)
    
        # Inicializa los costos acumulados
        accumulated_cost = { key_pair: 0.0 for key_pair in symbols_list}
    
        # Inicializa los calendarios de compras
        symbol_timetable = {key_pair: [] for key_pair in symbols_list}
    
        # Agrega por diccionario y por fecha
        for symbol, serie, utc_date, cost in result["fetched"]:
            key = (symbol,serie)
            accumulated_cost[key] += cost
            current_date = self._utc2date(utc_date)
            if init <= current_date and current_date <= end:
                symbol_timetable[key] += [(current_date, round(accumulated_cost[key],2))]
    
        # Devuelve las acciones de compra
        return symbol_timetable

    def consult_section_symbols(self, section_str):
        """Dado el nombre de una sección, devuelve las claves de los productos que
        pertenecen a ésta"""
    
        # Define la instrucción requerida en la consulta
        SQL_QUERY = """SELECT products.symbol, products.serie FROM buys
        JOIN products ON products.id = buys.symbol
        WHERE products.secc = ?
        GROUP BY buys.symbol HAVING SUM(buys.qty) > 0"""
    
        # Ejecuta la consulta y los placeholders deben acumularse
        result = self._execute_query(SQL_QUERY, [section_str])
    
        # Devuelve directamente la lista con la claves
        return result["fetched"]

    def bulk_insert_product(self, data_table, start_row=1):
        """Para una tabla con la información relevante, inserta cada fila en masa
        dentro de la base de datos. Esto se considegu
        e extrayendo la información de
        cada fila y organizándola en una tupla"""
    
        SQL_INSERT = "INSERT OR IGNORE INTO products(symbol,serie,src,secc) VALUES (?,?,?,?)"
    
        data = []
        current_section = ""
        for section, symbol, serie, source,_,_,_ in data_table[start_row:]:
            if section != '':
                current_section = section
            insert_row = (symbol, serie, source, current_section)
            data.append(insert_row)
    
        return self._execute_many(SQL_INSERT, data)

    @staticmethod
    def _bulk_op_processing(data_table, start_row=2, sign=1):
         """Al recibir la tabla, debe definirse si el valor de la transacción es
         positivo o negativo y si admitir sólo operaciones completadas. También se
         hacen ajustes menores a los tipos de datos para garantizar que sean los
         mismos que se tienen en la base de datos. La función se aisla porque el
         proceso se realiza sobre al menos dos tablas de la misma forma antes de
         continuar"""
    
         # Regenera las filas de tabla, transformando la información que se ingresa
         return [(str(symbol), str(serie), int(datetime.strptime(date, "%Y-%m-%d").timestamp()), sign*qty, sign*price)
                 for _, _, symbol, serie, date, status, qty, _, _, _, _, price,_ in data_table[start_row:]
                 if status == 'DONE']

    def bulk_insert_buys(self, buys_table, sells_table, start_row=2):
        """Para una tabla con la información relevante para una compra (si sign=1) o
        una venta (si sign=-1), inserta esa información dentro de la base de datos
        con una potencial modificación: Para insertar una fila con un elemento único
        se requiere símbolo y fecha de compra/venta. Esto quiere decir las filas
        deben acumularse antes de insertarse."""
    
        # Define el query requerida para la operación
        SQL_INSERT = "INSERT OR IGNORE INTO buys(symbol,qty,price,date) VALUES (?,?,?,?)"
    
        # Extrae los IDs de la base de datos
        ids_dictionary = self._symbols_ids()
    
        # Une los tickets de compra y venta en una lista
        tickets = self._bulk_op_processing(buys_table, start_row=start_row, sign=1) + self._bulk_op_processing(sells_table, start_row=start_row, sign=-1)
    
        # Acumula los valores de compra y venta diarios por symbol+serie+date usando
        # el ID de symbol+serie en la base de datos
        day_tickets = {}
        for symbol, serie, utc_timestamp, qty, price in tickets:
            ticket_key = (ids_dictionary[(symbol,serie)], utc_timestamp)
            ticket_qty_price = day_tickets.get(ticket_key, (0.0, 0.0))
            day_tickets[ticket_key] = tuple(a + b for a, b in zip(ticket_qty_price, (qty,price)))
    
        # Organiza la información acumulada para insertar la información
        data = [ (symbol_id, qty, price, utc_timestamp) for (symbol_id, utc_timestamp), (qty, price) in day_tickets.items() ]
    
        # Devuelve el resultado de ejecutar la query
        return self._execute_many(SQL_INSERT, data)

    def bulk_insert_prices(self, scraps_dictionary):
        # Define el query requerida para la operación
        SQL_INSERT = "INSERT OR IGNORE INTO prices(symbol,date,price) VALUES (?,?,?)"
    
        # Extrae los IDs de la base de datos
        ids_dictionary = self._symbols_ids()
    
        # Organiza las inserciones que debe realizarse como tuplas
        data = [ (ids_dictionary[symbol_key], self._date2utc(date) , price)
                 for symbol_key, prices_dictionary in scraps_dictionary.items()
                 for date, price in prices_dictionary.items()]
    
        return self._execute_many(SQL_INSERT, data)
