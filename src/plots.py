import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
from datetime import date, datetime, time, timedelta
import numpy as np

COLORS = ['yellow','green','blue','purple','red']

def _get_prev_monday (given_date):
    days_since_monday = given_date.weekday()
    given_monday = given_date - timedelta(days=days_since_monday)
    return given_monday

def _get_next_monday (given_date):
    days_to_monday = 7 - given_date.weekday()
    given_monday = given_date + timedelta(days=days_to_monday)
    return given_monday

def _split_list_with (given_list, split_list):

    # Revisa un caso extremo: sin puntos no hay corte
    if len(split_list) == 0:
        return [given_list]

    # Iniciando las piezas que resulten
    splitted = []

    # Se indica con cuál lista debe comenzarse
    splitting = given_list

    # Se usa cada elemento indicado para hacer la partición
    for split in split_list:

        # Se parte en dos el arreglo en turno
        left_split = [ x for x in splitting if x <= split]
        right_split = [ x for x in splitting if x >= split]

        # Y sólo se considera una pieza si no es vacío
        if len(left_split) != 0:
            splitted.append(left_split)

        # Se cambia el arreglo que se debe particionar como la segunda parte
        splitting = right_split

    # Al terminar, se agrega el residuo si es no vacío
    if len(splitting) != 0:
        splitted.append(splitting)

    # Devuelve las piezas reconocidas
    return splitted

def _distribute_vertically (pairs_list, threshold = 100):
    # Obtiene las posiciones a distribuir
    y_positions = [y_label_position for y_label_position, _ in pairs_list]

    # Ordena el arreglo original respecto a esas posiciones
    sorted_indices = sorted(range(len(y_positions)), key=y_positions.__getitem__)[::-1]
    sorted_pairs = [pairs_list[i] for i in sorted_indices]

    # La etiqueta más alta es el punto de partida
    new_pairs_list = [sorted_pairs[0]]

    # Para calcular el resto revisando si hay colisiones
    for i in range(1, len(sorted_pairs)):
        y_label_position, y_value = sorted_pairs[i]
        prev_y_label_position, _ = sorted_pairs[i-1]
        distance = prev_y_label_position - y_label_position
        if distance < threshold:
            new_y_label_position = y_label_position - threshold
        else:
            new_y_label_position = y_label_position
        new_pairs_list.append((new_y_label_position, y_value))

    # Regresa las modificaciones
    return new_pairs_list

def _plot_basic_settings(fig, ax):
    # Configura los elementos base
    fig.set_figwidth(13)
    ax.tick_params(axis='x', colors='white')
    ax.tick_params(axis='y', colors='white')
    ax.tick_params(axis='both', which='major', labelsize='small')
    ax.grid(which='major', linestyle=':', linewidth=0.8)
    ax.grid(which='minor', linestyle=':', linewidth=0.5)
    ax.spines['bottom'].set_color('white')
    ax.spines['top'].set_color('white')
    ax.spines['right'].set_color('white')
    ax.spines['left'].set_color('white')
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, pos: f'${x:,.0f}'))

def plot_value_history (symbols_values, buys_timetable={}, save_path = None):
    """Crea una imagen en archivo indicado con una gráfica que describe la
    evolución del activo, indicando las compras que se realizaron en ese periodo
    de tiempo"""

    # Crea los objetos que requerimos para la gráfica
    fig, ax = plt.subplots()

    # Configura la gráfica...
    _plot_basic_settings(fig,ax)

    # Cada elemento del diccionario...
    for i, key in enumerate(symbols_values.keys()):

        # Define una llave que extrae el nombre
        symbol, _ = key

        # Elige un color para la gráfica
        color = COLORS[i % len(COLORS)]

        # Extrae los ejes de la gráfica
        x_axis = [_get_prev_monday(value_date) for value_date in symbols_values[key].keys()]
        y_axis = symbols_values[key].values()

        # Grafica los valores
        ax.plot(x_axis, y_axis, linewidth=2.0, markeredgewidth=0.5, label=symbol, color=color)

    # Grafica las fechas de compra
    all_buy_dates = set([_get_next_monday(buy_date)
                         for buy_dates in buys_timetable.values()
                         for buy_date in list(buy_dates.keys())[1:]])
    for buy_date in all_buy_dates:
        ax.axvline(x=buy_date, color="olive", ls="--"  , lw=1, zorder=1)

    # Extrae los ticks generados para conocer los límites
    y_ticks = ax.get_yticks()

    # Define cuanto debe desplazarse la etiqueta
    label_y_shift = (y_ticks[-1] - y_ticks[0])/20

    # Elige posiciones para mostrar valores
    n = min([len(symbol_dict) for symbol_dict in symbols_values.values()])
    steps = [int((i/4) * n)-1 for i in range(1,5)]

    # Colección de textos para evitar colisiones
    symbol_key = next(iter(symbols_values))
    x_axis = list(symbols_values[symbol_key].keys())
    labels_info = {x_axis[i] : [] for i in steps}

    # Después de que todo está graficado, se crean etiquetas
    for i, key in enumerate(symbols_values.keys()):

        # Extrae los ejes de la gráfica
        x_axis = list(symbols_values[key].keys())
        y_axis = list(symbols_values[key].values())

        # Elige un color para la gráfica
        color = COLORS[i % len(COLORS)]

        # Marca los puntos en la gráfica
        ax.scatter([x_axis[i] for i in steps], [y_axis[i] for i in steps], marker='o', color=color)

        # Crea las etiquetas a mostrar
        for j in steps:

            # Se considera una primera posición
            x_value = x_axis[j]
            y_value = y_axis[j]

            # Si se excede el margen vertical, se dibuja debajo
            if y_value + 2*label_y_shift > y_ticks[-2]:
                label_y_position = y_value - label_y_shift
            else:
                label_y_position = y_value + label_y_shift

            # Guarda la posición potencial
            labels_info[x_value].append((label_y_position, y_value))

    for x_label in labels_info:
        x_value = x_label + timedelta(days=4)
        new_positions = _distribute_vertically(labels_info[x_label])
        for label_y_position, y_value in new_positions:
            text_obj = ax.text(x=x_value, y=label_y_position, s=f"{y_value:,.2f}", bbox=dict(facecolor="white"))

    # Ajusta la información a mostrar
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.1), ncol=6, fancybox=True, framealpha=0.0, labelcolor="white")
    fig.autofmt_xdate()

    # Genera la gráfica
    if save_path is None:
        ax.set_facecolor("black")
        fig.patch.set_facecolor("black")
        plt.show()
    else:
        plt.savefig(save_path, transparent = True)
    plt.close()

def plot_added_value_history (symbols_values, buys_timetable={}, buys_initial_value=0.0, save_path = None):

    # Crea los objetos que requerimos para la gráfica
    fig, ax = plt.subplots()

    # Configura la gráfica...
    _plot_basic_settings(fig,ax)

    # Select line color
    color = "teal"

    # Obtiene las fechas comunes en valores
    shortest_key = min(symbols_values.keys(), key=(lambda k: len(symbols_values[k])))
    common_dates = [ _get_prev_monday(value_date) for value_date in
                     symbols_values[shortest_key]]

    # Define las cotas de fechas que van a usarse
    init_date = min(common_dates)
    end_date = max(common_dates)

    # Acumula el valor de cada activo
    accumulated_value = {}
    for date_values in symbols_values.values():
        for value_date, value in date_values.items():
            value_monday = _get_prev_monday(value_date)
            if init_date <= value_monday and value_monday <= end_date:
                accumulated_value[value_monday] = accumulated_value.get(value_monday, 0.0) + value

    # Calcula el valor invertido al inicio
    init_buy_value = sum([buy_value for buy_value in buys_initial_value.values()])

    # Obtiene todas las fechas de compras y acumula por fechas individuales
    total_buys = {}
    for symbol_pair, buy_dates in buys_timetable.items():
        for buy_date, buy_value in buy_dates.items():
            next_monday = _get_next_monday(buy_date)
            total_buys[next_monday] = total_buys.get(next_monday, 0.0) + buy_value

    # Aglutina las compras de cada fecha de manera ordenada
    total_cost = init_buy_value
    for buy_date in sorted(total_buys.keys()):
        buy_value = total_buys[buy_date]
        total_cost += buy_value
        total_buys[buy_date] = round(total_cost,2)

    # Usando las fechas de compras, se parten las regiones para indicar los
    # cambios de valor repentinos provocados por una compra
    x_axis = list(accumulated_value.keys())
    x_fragments = _split_list_with(x_axis, list(total_buys.keys()))

    # Se debe coleccionar los valores en el otro eje
    y_fragments = []

    # Se barre sobre todos los fragmentos para obtener esos valores
    n = len(x_fragments)
    for i in range(len(x_fragments)):
        x_region = x_fragments[i]
        y_region = []

        # Cada fragmento define un valor, pero el último debe tener el valor que
        # lo antecede para generar una discontinuidad
        for j in range(len(x_region)-1):
            current_date = x_region[j]
            value = accumulated_value[current_date]
            y_region.append(round(value,2))

        # Pero esto solo se hace en los fragmentos interiores, el último no
        # tiene ese cambio
        if i != n-1:
            y_region.append(value)
        else:
            current_date = x_region[j+1]
            value = accumulated_value[current_date]
            y_region.append(round(value,2))

        # Esta colección se agrega a los fragmentos en y
        y_fragments.append(y_region)

    # Inicia la lista con ceros
    y_fragments_buys = [ [init_buy_value] * len(x_region) for x_region in x_fragments]

    # Revisa todas las fechas de compra
    for buy_date in total_buys.keys():
        for i,x_region in enumerate(x_fragments):
            #Asigna valor si se coincide en la primera fecha
            first_date = x_region[0]
            if first_date == buy_date:
                y_fragments_buys[i] = [ total_buys[buy_date] ] * len(x_region)

    # Etiqueta de la gráfica
    label = "+".join([symbol for symbol, _ in symbols_values])

    # Grafica las el valor principal
    for x_region, y_region in zip(x_fragments, y_fragments):
        ax.plot(x_region, y_region, linewidth=2.0, markeredgewidth=0.5, color=color)

    # Grafica el valor de compra total
    n = len(x_fragments)
    for i, (x_region, y_region) in enumerate(zip(x_fragments, y_fragments_buys)):
        if i != n-1:
            ax.plot(x_region, y_region, linewidth=1.0 ,color="red", ls="--")
        else:
            value = y_region[-1]
            ax.plot(x_region, y_region, linewidth=1.0 ,color="red", ls="--", label=f"Inversión: {value:,.2f}")

    # Grafica las fechas de compra
    for buy_date in total_buys.keys():
        ax.axvline(x=buy_date, color="white", ls="--"  , lw=1, zorder=1)


    # Extrae los ticks generados para conocer los límites
    y_ticks = ax.get_yticks()

    # Define cuanto debe desplazarse la etiqueta
    label_y_shift = (y_ticks[-1] - y_ticks[0])/15

    # Elige posiciones para mostrar valores
    n = len(x_axis)
    steps = [x_axis[int((i/4) * n)-1] for i in range(1,5)]

    # Grafica los puntos claves
    label_y_values = [accumulated_value[x_value] for x_value in steps]
    ax.scatter(steps, label_y_values, marker='o', color=color)

    # Crea las etiquetas a mostrar
    for x_value in steps:

        # Se obtiene el valor graficado
        y_value = accumulated_value[x_value]

        # Si se excede el margen vertical, se dibuja debajo
        if y_value + 2*label_y_shift > y_ticks[-2]:
            label_y_position = y_value - label_y_shift
        else:
            label_y_position = y_value + label_y_shift

        # Dibuja las etiquetas
        ax.text(x=x_value + timedelta(days=5), y=label_y_position, s=f"{y_value:,.2f}", bbox=dict(facecolor="white"))

    # Agrega información extra
    current_value = y_fragments[-1][-1]
    buy_value = y_fragments_buys[-1][-1]
    roe = current_value - buy_value
    roi = (roe / buy_value)*100
    ax.text(0.0, 1.1, f"ROE: {roe:,.2f}\nROI: {roi:,.2f}%", horizontalalignment='left', verticalalignment='top', transform=ax.transAxes, color="white")

    # Ajusta la información a mostrar
    ax.set_title(label, color="white")
    ax.legend(loc='upper left', bbox_to_anchor=(0.7, 1.1), ncol=2, fancybox=True, framealpha=0.0, labelcolor="white")
    fig.autofmt_xdate()

    # Genera la gráfica
    if save_path is None:
        ax.patch.set_facecolor("black")
        fig.patch.set_facecolor("black")
        plt.show()
    else:
        plt.savefig(save_path, transparent = True)
    plt.close()

def plot_general_distribution(sections_values, **kwargs):

    # Extrae información
    pair_values = [(section,value) for section, value in sections_values.items() if value != 0.0]
    labels, data = list(zip(*pair_values))

    # LLama a la gráfica con la información extraída
    plot_pie_chart(labels, data, **kwargs)

def plot_local_distribution(symbols_values, **kwargs):

    #Extra la información relevante
    pair_values = [(symbol, dates_values["value"])
                   for (symbol, _), dates_values in symbols_values.items() if dates_values["value"] != 0.0]
    labels, data = list(zip(*pair_values))

    # LLama a la gráfica con la información extraída
    plot_pie_chart(labels, data, **kwargs)

def plot_pie_chart (labels, data, save_path=None, angle=-40):

    # Calcula el total de inversión
    total = sum(data)

    # Define marco y ejes
    fig, ax = plt.subplots()

    # Crea la gráfica de pie
    wedges, texts, pcts = ax.pie(data, radius=1, textprops=dict(color='white',size='smaller'),
                                 wedgeprops=dict(width=0.6, edgecolor='w'), autopct='%.0f%%',
                                 startangle=angle)

    ax.set_title(f'Total: ${total:,.2f}', color='white')

    # Crea las etiquetas de la gráfica
    bbox_props = dict(boxstyle="square,pad=0.3", fc="w", ec="k", lw=0.72)
    kw = dict(arrowprops=dict(arrowstyle="-", edgecolor="white"),
              bbox=bbox_props, zorder=0, va="center")

    for i, p in enumerate(wedges):
        ang = (p.theta2 - p.theta1)/2. + p.theta1
        y = np.sin(np.deg2rad(ang))
        x = np.cos(np.deg2rad(ang))
        horizontalalignment = {-1: "right", 1: "left"}[int(np.sign(x))]
        connectionstyle = f"angle,angleA=0,angleB={ang}"
        kw["arrowprops"].update({"connectionstyle": connectionstyle})
        ax.annotate(f"{labels[i]}\n${data[i]:,.2f}", xy=(x, y), xytext=(1.35*np.sign(x), 1.4*y),
                    horizontalalignment=horizontalalignment, **kw)


    # Genera la gráfica
    if save_path is None:
        ax.patch.set_facecolor("black")
        fig.patch.set_facecolor("black")
        plt.show()
    else:
        plt.savefig(save_path, transparent = True)
    plt.close()
