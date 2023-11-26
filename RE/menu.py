import questionary
import Stream_Processor
import Save_To_Mongodb
import Data_From_Mongo
import Export_To_PowerBi
import Convert_To_Pandas
import Numpy_Stats
import Upload_To_HDFS
import Data_From_Mongo_SQL

class DataProcessingApp:
    def __init__(self):
        self.menu_options = {
            "Usar Spark Streaming": self.Stream_Processor,
            "Guardar datos en MongoDB": self.Save_To_Mongodb,
            "Extraer datos de mongo mediante agregaciones": self.Data_From_Mongo,
            "Extraer datos usando SQL": self.Data_From_Mongo_SQL,
            "Convertir a Pandas": self.Convert_To_Pandas,
            "Estadísticas con Numpy": self.Numpy_Stats,
            "Exportar datos para PoweBi": self.Export_To_PowerBi,
            "Subir ficheros a HDFS": self.Upload_To_HDFS,
        }

    def run(self):
        respuesta = questionary.select(
            "Elige una opción:",
            choices=list(self.menu_options.keys())
        ).ask()

        print(f"Has seleccionado: {respuesta}")
        self.menu_options[respuesta]()

    def Stream_Processor(self):
        Stream_Processor.main()

    def Save_To_Mongodb(self):
        Save_To_Mongodb.main()

    def Data_From_Mongo(self):
        Data_From_Mongo.main()

    def Export_To_PowerBi(self):
        Export_To_PowerBi.main()

    def Convert_To_Pandas(self):
        Convert_To_Pandas.main()

    def Numpy_Stats(self):
        Numpy_Stats.main()

    def Upload_To_HDFS(self):
        Upload_To_HDFS.main()
    
    def Data_From_Mongo_SQL(self):
        Data_From_Mongo_SQL.main()

if __name__ == "__main__":
    app = DataProcessingApp()
    app.run()
