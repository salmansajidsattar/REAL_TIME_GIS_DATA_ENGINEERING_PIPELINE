# src/visualization/dashboard.py
import dash
from dash import dcc, html, Input, Output, callback
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import logging
from config.config import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EarthquakeDashboard:
    def __init__(self):
        self.config = Config()
        self.app = dash.Dash(__name__)
        self.setup_layout()
        self.setup_callbacks()
    
    def get_database_connection(self):
        """Get database connection"""
        return psycopg2.connect(
            host=self.config.DB_HOST,
            port=self.config.DB_PORT,
            database=self.config.DB_NAME,
            user=self.config.DB_USER,
            password=self.config.DB_PASSWORD
        )
    
    def fetch_earthquake_data(self, hours=24):
        """Fetch earthquake data from database"""
        try:
            conn = self.get_database_connection()
            
            # Query for recent earthquakes
            query = """
            SELECT id, magnitude, place, timestamp, longitude, latitude, depth,
                   magnitude_category, depth_category, event_datetime, created_at
            FROM earthquakes 
            WHERE created_at >= %s
            ORDER BY timestamp DESC
            LIMIT 1000
            """
            
            cutoff_time = datetime.now() - timedelta(hours=hours)
            
            df = pd.read_sql_query(query, conn, params=[cutoff_time])
            conn.close()
            
            return df
            
        except Exception as e:
            logger.error(f"Error fetching earthquake data: {e}")
            return pd.DataFrame()
    
    def setup_layout(self):
        """Setup dashboard layout"""
        self.app.layout = html.Div([
            html.H1("Real-time Earthquake Monitoring Dashboard", 
                   style={'textAlign': 'center', 'marginBottom': 30}),
            
            # Auto-refresh component
            dcc.Interval(
                id='interval-component',
                interval=30*1000,  # Update every 30 seconds
                n_intervals=0
            ),
            
            # Time range selector
            html.Div([
                html.Label("Time Range:"),
                dcc.Dropdown(
                    id='time-range-dropdown',
                    options=[
                        {'label': 'Last Hour', 'value': 1},
                        {'label': 'Last 6 Hours', 'value': 6},
                        {'label': 'Last 24 Hours', 'value': 24},
                        {'label': 'Last Week', 'value': 168}
                    ],
                    value=24,
                    style={'width': '200px'}
                )
            ], style={'margin': '20px'}),
            
            # Summary statistics
            html.Div(id='summary-stats', style={'margin': '20px'}),
            
            # World map
            html.Div([
                html.H3("Earthquake Locations"),
                dcc.Graph(id='earthquake-map')
            ], style={'margin': '20px'}),
            
            # Magnitude distribution
            html.Div([
                html.H3("Magnitude Distribution"),
                dcc.Graph(id='magnitude-histogram')
            ], style={'width': '48%', 'display': 'inline-block'}),
            
            # Depth distribution
            html.Div([
                html.H3("Depth Distribution"),
                dcc.Graph(id='depth-histogram')
            ], style={'width': '48%', 'float': 'right', 'display': 'inline-block'}),
            
            # Time series
            html.Div([
                html.H3("Earthquake Activity Over Time"),
                dcc.Graph(id='time-series')
            ], style={'margin': '20px'}),
            
            # Recent earthquakes table
            html.Div([
                html.H3("Recent Earthquakes"),
                html.Div(id='recent-earthquakes-table')
            ], style={'margin': '20px'})
        ])
    
    def setup_callbacks(self):
        """Setup dashboard callbacks"""
        @self.app.callback(
            [Output('summary-stats', 'children'),
             Output('earthquake-map', 'figure'),
             Output('magnitude-histogram', 'figure'),
             Output('depth-histogram', 'figure'),
             Output('time-series', 'figure'),
             Output('recent-earthquakes-table', 'children')],
            [Input('interval-component', 'n_intervals'),
             Input('time-range-dropdown', 'value')]
        )
        def update_dashboard(n, time_range):
            # Fetch data
            df = self.fetch_earthquake_data(hours=time_range)
            
            if df.empty:
                empty_fig = go.Figure()
                empty_fig.add_annotation(text="No data available", 
                                       xref="paper", yref="paper",
                                       x=0.5, y=0.5, showarrow=False)
                return (
                    html.Div("No data available"),
                    empty_fig, empty_fig, empty_fig, empty_fig,
                    html.Div("No recent earthquakes")
                )
            
            # Summary statistics
            total_earthquakes = len(df)
            avg_magnitude = df['magnitude'].mean()
            max_magnitude = df['magnitude'].max()
            
            summary_stats = html.Div([
                html.Div([
                    html.H4(f"{total_earthquakes}"),
                    html.P("Total Earthquakes")
                ], className='stat-box', style={'display': 'inline-block', 'margin': '10px', 'padding': '10px', 'border': '1px solid #ccc'}),
                
                html.Div([
                    html.H4(f"{avg_magnitude:.2f}"),
                    html.P("Average Magnitude")
                ], className='stat-box', style={'display': 'inline-block', 'margin': '10px', 'padding': '10px', 'border': '1px solid #ccc'}),
                
                html.Div([
                    html.H4(f"{max_magnitude:.2f}"),
                    html.P("Maximum Magnitude")
                ], className='stat-box', style={'display': 'inline-block', 'margin': '10px', 'padding': '10px', 'border': '1px solid #ccc'})
            ])
            
            # World map
            map_fig = px.scatter_mapbox(
                df, lat='latitude', lon='longitude', 
                size='magnitude', color='magnitude',
                hover_name='place', hover_data=['magnitude', 'depth'],
                color_continuous_scale='YlOrRd',
                size_max=15, zoom=1
            )
            map_fig.update_layout(
                mapbox_style="open-street-map",
                height=500,
                margin={"r":0,"t":0,"l":0,"b":0}
            )
            
            # Magnitude histogram
            mag_hist = px.histogram(
                df, x='magnitude', nbins=20,
                title="Distribution of Earthquake Magnitudes"
            )
            
            # Depth histogram
            depth_hist = px.histogram(
                df, x='depth', nbins=20,
                title="Distribution of Earthquake Depths"
            )
            
            # Time series
            df['event_datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
            time_series_data = df.groupby(df['event_datetime'].dt.floor('H')).size().reset_index()
            time_series_data.columns = ['hour', 'count']
            
            time_series_fig = px.line(
                time_series_data, x='hour', y='count',
                title="Earthquake Activity Over Time"
            )
            
            # Recent earthquakes table
            recent_earthquakes = df.head(10)[['place', 'magnitude', 'depth', 'event_datetime']].to_dict('records')
            
            table_rows = []
            for eq in recent_earthquakes:
                table_rows.append(html.Tr([
                    html.Td(eq['place']),
                    html.Td(f"{eq['magnitude']:.1f}"),
                    html.Td(f"{eq['depth']:.1f} km"),
                    html.Td(eq['event_datetime'].strftime('%Y-%m-%d %H:%M:%S') if pd.notna(eq['event_datetime']) else 'N/A')
                ]))
            
            recent_table = html.Table([
                html.Thead([
                    html.Tr([
                        html.Th('Location'),
                        html.Th('Magnitude'),
                        html.Th('Depth'),
                        html.Th('Time')
                    ])
                ]),
                html.Tbody(table_rows)
            ], style={'width': '100%', 'border': '1px solid #ccc'})
            
            return (
                summary_stats,
                map_fig,
                mag_hist,
                depth_hist,
                time_series_fig,
                recent_table
            )
    
    def run(self, debug=False, host='0.0.0.0', port=8050):
        """Run the dashboard"""
        logger.info(f"Starting dashboard on http://{host}:{port}")
        self.app.run_server(debug=debug, host=host, port=port)

if __name__ == "__main__":
    dashboard = EarthquakeDashboard()
    dashboard.run(debug=True)