import sqlite3
from datetime import date, datetime, time

class FinancialDB:
    """Clase sencilla para el mantenimiento de los datos extraídos por los
    scrappers"""
    db_path = None

    def __init__ (self, filepath):
        """Constructor que define el nombre del archivo de la base de datos"""
    
        self.db_path = filepath

    def _execute (self, calling_function):
        """Una evoltura para ~execute~ en SQLite para manejar los posibles problemas
        y aislar los componentes de conexión"""
    
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            calling_function(cursor)
            conn.commit()
            return { 'fetched' : cursor.fetchall(),
                     'rowcount': cursor.rowcount,
                     'lastrowid': cursor.lastrowid }
    
        except sqlite3.Error as error:
            return error
    
        finally:
            if conn:
                conn.close()

    def _execute_query (self, query_str, parameters=()):
        """Una evoltura para ~execute_many~ en SQLite para manejar los posibles
        problemas de manera externa"""
        return self._execute(lambda cur: cur.execute(query_str, parameters))

    def _execute_many (self, query_str, parameters):
        """Una evoltura para ~execute_many~ en SQLite para manejar los posibles
        problemas de manera externa"""
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

    def scrap_consult (self, symbols_list):
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
        return { (symbol, serie) : datetime.utcfromtimestamp(str_date).date()
                 for symbol, serie, str_date in result["fetched"]}

    def insert_product_bulk(self, data_table, start_row=1):
        """Para una tabla con la información relevante, inserta cada fila en masa
        dentro de la base de datos. Esto se considegue extrayendo la información de
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
    def _op_processing(data_table, start_row=2, sign=1):
         """Al recibir la tabla, debe definirse si el valor de la transacción es
         positivo o negativo y si admitir sólo operaciones completadas. También se
         hacen ajustes menores a los tipos de datos para garantizar que sean los
         mismos que se tienen en la base de datos. La función se aisla porque el
         proceso se realiza sobre al menos dos tablas de la misma forma antes de
         continuar"""
    
         # Genera una nueva fila con las condiciones necesarias
         return [(str(symbol), str(serie), int(datetime.strptime(date, "%Y-%m-%d").timestamp()), sign*qty, sign*price)
                 for _, _, symbol, serie, date, status, qty, _, _, _, _, price,_ in data_table[start_row:]
                 if status == 'DONE']

    def insert_buys_bulk(self, buys_table, sells_table, start_row=2):
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
        tickets = self._op_processing(buys_table, start_row=start_row, sign=1) + self._op_processing(sells_table, start_row=start_row, sign=-1)
    
        # Acumula los valores de compra y venta por symbol+date usando el ID en la
        # base de datos
        day_tickets = {}
        for symbol, serie, date, qty, price in tickets:
            ticket_key = (ids_dictionary[(symbol,serie)], date)
            ticket_qty_price = day_tickets.get(ticket_key, (0.0, 0.0))
            day_tickets[ticket_key] = tuple(a + b for a, b in zip(ticket_qty_price, (qty,price)))
    
        # Organiza la información acumulada para insertar la información
        data = [ (symbol_id, qty, price, date) for (symbol_id, date), (qty, price) in day_tickets.items() ]
    
        # Devuelve el resultado de ejecutar la query
        return self._execute_many(SQL_INSERT, data)

    def insert_scrap_prices(self, scraps_dictionary):
        # Define el query requerida para la operación
        SQL_INSERT = "INSERT OR IGNORE INTO prices(symbol,date,price) VALUES (?,?,?)"
    
        # Extrae los IDs de la base de datos
        ids_dictionary = self._symbols_ids()
    
        # Organiza las inserciones que debe realizarse como tuplas
        data = [ (ids_dictionary[symbol_key], int(datetime.combine(date, time.min).timestamp()), price)
                 for symbol_key, prices_dictionary in scraps_dictionary.items()
                 for date, price in prices_dictionary.items()]
    
        return self._execute_many(SQL_INSERT, data)
