"""
TRIADB-TriaClick Demo with Northwind Traders DataModel/DataSet
Sales Scenario - Order Subtotals By Product

Buid a dashboard with:
5 dimensions: Customer Country, Product Category, Employee Title, Employee Last, Shipped Year
2 tables: Products, Subtotals By Product
1 barchart: to visualize Subtotals By Product

Selections to try:
1: Sales Representatives, for Dairy Products, in 1996
(use 1 to verify results from Jupyter Notebook and compare also with QlikView)
2: Grains/Cereals, for Sales Representative, in 1994

(C) October 2019 By Athanassios I. Hatzis
"""
import dash
import dash_table
import dash_html_components as html
from dash.dependencies import Input, Output
import dash_core_components as dcc
import plotly.graph_objs as go
import pandas as pd

from triadb import MIS

mis = MIS(debug=1)
mis.connect_to_datastore(dbms='clickhouse', host='localhost', port=9000,
                         user='demo', password='demo', database='TriaDB', trace=0)
mis.connect_to_metastore(dbms='mariadb', host='localhost', port=3306,
                         user='demo', password='demo', database='TRIADB', trace=0)
eng = mis.restart(model_dim=100, reset=False)

btnclicks_counter = 0

table_components = [
    {'key': 65, 'title': 'Customer Country', 'label': 'customer_country', 'dataframe': None},
    {'key': 14, 'title': 'Product Category', 'label': 'product_category', 'dataframe': None},
    {'key': 42, 'title': 'Employee Title', 'label': 'employee_title', 'dataframe': None},
    {'key': 40, 'title': 'Employee Last', 'label': 'employee_last', 'dataframe': None},
    {'key': 16, 'title': 'Products', 'label': 'dairy_products', 'dataframe': None},
    {'key': 24, 'title': 'Subtotals by Product', 'label': 'subtotals', 'dataframe': None}
]


def read_data():
    for d in table_components:
        if d['key'] == 16:
            if eng.get_aset(16).filtered:
                d['dataframe'] = mis.get_tuples(73, 17, 20, aset_dim2=16,
                                                order_by='p_stock DESC', pandas_columns='ID, Name, Stock')
            else:
                d['dataframe'] = pd.DataFrame(columns=['ID', 'Name', 'Stock'])
        elif d['key'] == 24:
            if eng.get_aset(24).filtered:
                d['dataframe'] = mis.get_tuples(73, 25, 26, 27, aset_dim2=24,
                                                projection='concat(\':\', toString(any(p_id))) AS product_id, '
                                                'round(sum(odet_price*odet_quantity*(1-odet_discount)), 1) AS subtotal',
                                                group_by='p_id', order_by='subtotal DESC', limit=10,
                                                pandas_columns='ProductID, SubTotal')
            else:
                d['dataframe'] = pd.DataFrame(columns=['ProductID', 'SubTotal'])
        else:
            d['dataframe'] = mis.get_items(dim2=d['key'], highlight=False)


read_data()


def data_table_params(table_id, df, height=800, width=350):
    d = {
        'id': table_id,
        'data': df.to_dict('records'),
        'columns': [{"name": label, "id": label} for label in df.columns],
        'row_selectable': 'single',
        'style_table': {'maxHeight': f'{height}px', 'width': f'{width}px',
                        'maxWidth': f'{width}px', 'overflowY': 'scroll', 'overflowX': 'scroll'},
        'style_header': {'backgroundColor': 'rgb(230, 230, 230)', 'fontWeight': 'bold'},
        'style_cell': {'textAlign': 'left'},
        'style_cell_conditional':
        [
            {'if': {'column_id': [label for label in df.columns][0]},
             'width': '70%'},
            {'if': {'column_id': 'FREQ'},
             'width': '10%'},
            {'if': {'column_id': 'S'},
             'width': '10%'},
            {'if': {'column_id': 'P'},
             'width': '10%'}
        ],
        'style_data_conditional':
        [
            {'if': {'filter_query': '{P} = 0'},
             'backgroundColor': 'lightgrey',
             'color': 'black'},
            {'if': {'filter_query': '{S} = 1'},
             'backgroundColor': 'lime',
             'color': 'black'},
        ]
    }
    return d


def create_datatable_component(title, label, data):
    return html.Div([html.H6(title),
                     dash_table.DataTable(**data_table_params(label, data))], className="three columns")


def create_components():
    l = [create_datatable_component(d['title'], d['label'], d['dataframe']) for d in table_components]
    l.append(html.Div(
        [
            dcc.Graph(
                figure=go.Figure(
                    data=[
                        go.Bar(
                            x=table_components[5]['dataframe']['ProductID'].to_list(),
                            y=table_components[5]['dataframe']['SubTotal'].to_list(),
                            name='Subtotals',
                        )
                    ],
                    layout=go.Layout(title='Subtotals by Product')
                ),
                style={'height': 450, 'width': 450},
                id='my-graph'
            )
        ], className="three columns"))

    return html.Div(l, className="row")


app = dash.Dash(__name__)

app.layout = html.Div([
    html.Div(
        [
            html.H3(children='TRIADB - TriaClick Associative Semiotic Hypergraph Engine',
                    className='twelve columns'),

            html.H5(children='Demo on Northwind Database with Interactive Associative Selection and Filtering',
                    className='twelve columns'),
        ], className="six columns"),

    html.Div(
        [
            html.Button(id='reset-button', n_clicks=0, children='Reset Filtering', autoFocus=True,
                        title='Press Button to Reset Filtering', className='button'),
            html.H6('Shipped Year'),
            dcc.Dropdown(
                id='shipped-year',
                options=[
                    {'label': '1993', 'value': 1993},
                    {'label': '1994', 'value': 1994},
                    {'label': '1995', 'value': 1995},
                    {'label': '1996', 'value': 1996},
                ],
                value=None
            )
        ], className="six columns"),

    html.Div(id='dimension-lists', children=create_components(),
             className="row")
])


@app.callback(
    Output(component_id='dimension-lists', component_property='children'),

    [Input(component_id='reset-button', component_property='n_clicks'),
     Input(component_id='shipped-year', component_property='value'),
     Input(component_id='product_category', component_property='selected_rows'),
     Input(component_id='employee_title', component_property='selected_rows')
     ]
)
def update_dimension_lists1(n_clicks, drop_down_value, sel_row1, sel_row2):
    global btnclicks_counter
    if n_clicks > btnclicks_counter:
        print('Reset Filtering button is pressed')
        btnclicks_counter = btnclicks_counter + 1
        mis.restart(100, reset=True)
        # Read data for the filtered table_components
        read_data()
    elif sel_row1:
        print('Product Category is selected')
        value = table_components[1]['dataframe'].iloc[sel_row1[0]][0]
        key = table_components[1]['key']
        # Select values, apply the filter
        mis.select(f"$v='{value}'", dim2=key)
        # Read data for the filtered dimensions
        read_data()
    elif sel_row2:
        print('Employee Title is selected')
        value = table_components[2]['dataframe'].iloc[sel_row2[0]][0]
        key = table_components[2]['key']
        # Select values, apply the filter
        mis.select(f"$v='{value}'", dim2=key)
        # Read data for the filtered dimensions
        read_data()
    elif drop_down_value:
        print('Shipped Year drop down value is chosen')
        # Select values, apply the filter
        mis.select(f'toYear($v)={drop_down_value}', dim2=31)
        # Read data for the filtered dimensions
        read_data()

    print(f'=========== Waiting for user input .... ===============')

    return create_components()


if __name__ == '__main__':
    app.run_server(port=8999, debug=False, dev_tools_ui=False, dev_tools_hot_reload=False)
