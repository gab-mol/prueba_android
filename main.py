'''
# Antotador personal. 
Intento de aplicación diseñada para android.

'''
# Kivy imports
from kivy.config import Config
## Configuración de dimensiones de ventana
Config.set('graphics', 'width', '500')
Config.set('graphics', 'height', '700')
from kivy.core.window import Window
from kivy.lang import Builder
from kivy.properties import DictProperty, ListProperty, StringProperty
from kivymd.app import MDApp
from kivymd.uix.screenmanager import MDScreenManager
from kivymd.uix.label import MDLabel

# other modules
import os
from configparser import ConfigParser
import sqlalchemy as sqla
import pandas as pd
from datetime import datetime, timezone
# KV code         ####     ####     ####
KV = '''
<ScManag>:
    peso: peso_tf.text
    dso_mx: so_mx_tf.text
    dso_mn: so_mn_tf.text
    dbo_mx: bo_mx_tf.text
    dbo_mn: bo_mn_tf.text
    Screen:
        name: "corp_mes"
        MDTopAppBar:
            title: "[size="+app.wresize["bar_fsize"]+"]Medidas[/size]"
            size_hint: 1, .1
            right_action_items: [["content-save", lambda x: root.save_mes()]]
            pos_hint: {'top': 1}
        BoxLayout:
            id: fields_cont
            size_hint: 1, .9
            orientation: 'vertical'
            padding: 15
            spacing: 7
            # MDLabel:
            #     size_hint: 1, .1
            #     pos_hint: {'top': 1}
            #     text: "Prueba00"
            #     font_size: 50
            #     theme_text_color: "Custom"
            #     text_color: app.COLS["purple"]
            MDTextField:
                id: peso_tf
                size_hint: 1, .08
                hint_text: "Peso (g)"
                mode: "rectangle"
                font_size: app.wresize["bar_fsize"]
                line_color_normal: app.theme_cls.accent_color
            MDTextField:
                id: so_mx_tf
                size_hint: 1, .08
                mode: "rectangle"
                font_size: app.wresize["bar_fsize"]
                hint_text: "Diámetro SO max (cm)"
                line_color_normal: app.theme_cls.accent_color
            MDTextField:
                id: so_mn_tf
                size_hint: 1, .08
                mode: "rectangle"
                font_size: app.wresize["bar_fsize"]
                hint_text: "Diámetro SO max (cm)"
                line_color_normal: app.theme_cls.accent_color
            MDTextField:
                id: bo_mx_tf
                size_hint: 1, .08
                mode: "rectangle"
                font_size: app.wresize["bar_fsize"]
                hint_text: "Diámetro BO max (cm)"
                line_color_normal: app.theme_cls.accent_color
            MDTextField:
                id: bo_mn_tf
                size_hint: 1, .08
                mode: "rectangle"
                font_size: app.wresize["bar_fsize"]
                hint_text: "Diámetro BO max (cm)"
                line_color_normal: app.theme_cls.accent_color
'''

Builder.load_string(KV)

# Connection          ####     ####     ####
class DbCon:
    def __init__(self) -> None:
        RUTA_CFG = os.path.join(os.path.dirname(__file__), "config.ini")
        config = ConfigParser()
        config.read(RUTA_CFG)
        bd_cred = config["DBcredent"]
        usu, cont, host = bd_cred["user"], bd_cred["pwd"], bd_cred["host"]
        puet, self.bd, self.squ = bd_cred["port"], bd_cred["dbname"], bd_cred["schema"]
        
        url = f"postgresql://{usu}:{cont}@{host}:{puet}/{self.bd}?sslmode=require"
        self.engine = sqla.create_engine(url,
                                connect_args={"options": f"-c search_path={self.squ}"}
                                )
        
        with self.engine.connect() as con:
            if self.squ not in con.dialect.get_schema_names(con):
                print("\nESQUEMA NO EXISTE, CREAR...")
                try:
                    con.execute(sqla.text(f"CREATE SCHEMA {self.squ}"))
                    con.commit()
                except:
                    raise Exception("ERROR AL CREAR ESQUEMA PARA TABLAS")
                
    def create_tb(self, nomb:str,cols_type:dict, id_auto=True):
        
        '''
        Crea la tabla en base de datos con el nombre 
        y columnas/tipo dato especificado, si esta NO existe.

        ### Parameters
            - nomb: str nombre de tabla
            - cols_type: dict claves= nombre.col | valores = tipo.dato POSGRESQL
            - id_auto: defoult: True. Controla si clave primaria es autoincremental o \
debe proporcionarse (luego del tipo de dato en `cols_type`).
        '''
        
        D_l = [f"{k} {cols_type[k]}" for k in cols_type]
        cols_q = ",\n".join(D_l)

        if id_auto:
            opc = "ID SERIAL PRIMARY KEY,"
        else:
            opc = ""

        try:
            with self.engine.begin() as con:
                con.execute(sqla.text(f"CREATE TABLE IF NOT EXISTS {nomb}(\n\
{opc}\n{cols_q})"))
                con.commit()
            print(f"\nTabla: {nomb} (DB: {self.bd}; \
schem: {self.squ}) = DISPONIBLE\n")
        except:
            Exception("PostgreSQL error")
        
    def send_df(self, nomb:str, df:pd.DataFrame, method=None):

        '''Enviar dataframe a tabla especificada de la base.

        args
            nomb: nombre de la tabla.
            df: dataframe a cargar.
            method: {None, 'multi', 'callable'}, defoult: None. Parámetro de 
            `pandas.DataFrame.to_sql`.'''

        try:
            with self.engine.connect() as con, con.begin():
                print("Conectando con postgreSQL...")
                print(df)
                df.to_sql(
                    name=nomb,
                    con=con,
                    schema=self.squ,
                    if_exists="append",
                    index=False,
                    method=method,
                    chunksize=1000
                )
        except:
            raise Exception("Error de carga: pandas.Dataframe >> postgreSQL")

    def sql_query(self, query:str, commit=True):

        '''Crea conexión con el motor instanciado y 
        envia query.

        args
            query: str con sentencia SQL.
            commit: aplicar método `sqlalchemy.Connection.commit()`, defoult: True
            '''

        try:
            with self.engine.connect() as con, con.begin():
                con.execute(sqla.text(query))
                if commit: con.commit()
        except:
            raise Exception("Error al ejecutar sentencia SQL")

# Kivy classes          ####     ####     ####
class ScManag(MDScreenManager):
    peso = StringProperty()
    dso_mx = StringProperty()
    dso_mn = StringProperty()
    dbo_mx = StringProperty()
    dbo_mn = StringProperty()

    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.app = MDApp.get_running_app()
        self.db_con = DbCon()
        self.tb_name = "medidas_diarias"
        # Preparar tabla destino
        self.db_con.create_tb(
            nomb = self.tb_name,
            cols_type = {
                "t_stamp": "TIMESTAMPTZ PRIMARY KEY",
                "peso": "FLOAT",
                "diametro_dso_mx":"FLOAT",
                "diametro_dso_mn":"FLOAT",
                "diametro_dbo_mx":"FLOAT",
                "diametro_dbo_mn":"FLOAT"
            },
            id_auto = False
        )
        

    # Screen: 'corp_mes' methods        ####
    def save_mes(self):
        print("prueba:\n",self.peso, self.dso_mx, self.dso_mn,
            self.dbo_mx, self.dbo_mn)        
        self.db_con.sql_query(
            query=f'''
            INSERT INTO {self.tb_name} (t_stamp, peso, diametro_dso_mx, 
            diametro_dso_mn, diametro_dbo_mx, diametro_dbo_mn)
            VALUES (
                '{datetime.now(timezone.utc)}',
                {self.peso},
                {self.dso_mx},
                {self.dso_mn},
                {self.dbo_mx},
                {self.dbo_mn}
            )
            '''
        )
            
class MedidasApp(MDApp):
    
    COLS = DictProperty()
    wresize = DictProperty()
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        # valores relativos
        self.wresize["bar_fsize"] = str(int(Window.size[1]/15))
        self.wresize["input_font_s"] = str(int(Window.size[1]/15))
        Window.bind(on_resize=self.on_resize)

        # colores (rgb 0-1)
        self.COLS = {
            "purple": (.611764705882353, 
            .15294117647058825, .6901960784313725)
        }
            
    def on_resize(self, *args):
        '''Al cambiar dimensiones de ventana'''
        self.wresize["bar_fsize"] = str(int(Window.size[1]/15))
        print(self.wresize["bar_fsize"])

    def build(self):
        self.theme_cls.theme_style = "Dark"
        self.theme_cls.primary_palette = "Purple"

        return ScManag()


if __name__ == "__main__":
    MedidasApp().run()

