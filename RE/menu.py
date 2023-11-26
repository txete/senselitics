import questionary
import Stream_Processor
import Save_To_Mongodb
import Data_From_Mongo
import Export_To_PowerBi

class DataProcessingApp:
    def __init__(self):
        self.menu_options = {
            "Usar Spark Streaming": self.Stream_Processor,
            "Guardar datos en MongoDB": self.Save_To_Mongodb,
            "Extraer datos de mongo mediante agregaciones": self.Data_From_Mongo,
            "Exportar datos para PoweBi": self.Export_To_PowerBi,
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

if __name__ == "__main__":
    app = DataProcessingApp()
    app.run()
