import questionary
import data_processing
import data_analysis
import data_hdfs_storage
import data_visualization

class DataProcessingApp:
    def __init__(self):
        self.menu_options = {
            "Procesamiento de Datos en Tiempo Real con Spark Streaming": self.data_processing,
            "Análisis de Datos con PySpark y Numpy": self.data_analysis,
            "Almacenamiento de Resultados en HDFS": self.hdfs_storage,
            "Visualización de Datos para powerBI y BigML": self.data_visualization,
            "Gráficos y Visualizaciones con Matplotlib": self.graphics_matplotlib,
            "Creación de Modelos de Machine Learning con BigML": self.ml_model_creation,
            "Reproducción de Predicciones con Librerías de IA": self.prediction_reproduction,
            "Análisis y Justificación de la IA Utilizada": self.ai_analysis_justification
        }

    def run(self):
        respuesta = questionary.select(
            "Elige una opción:",
            choices=list(self.menu_options.keys())
        ).ask()

        print(f"Has seleccionado: {respuesta}")
        self.menu_options[respuesta]()

    def data_processing(self):
        data_processing.main()

    def data_analysis(self):
        data_analysis.main()

    def hdfs_storage(self):
        data_hdfs_storage.main()

    def data_visualization(self):
        data_visualization.main()

    def graphics_matplotlib(self):
        print("Creando gráficos con Matplotlib...")

    def ml_model_creation(self):
        print("Creando modelos de Machine Learning con BigML...")

    def prediction_reproduction(self):
        print("Reproduciendo predicciones con librerías de IA...")

    def ai_analysis_justification(self):
        print("Analizando y justificando el uso de la IA...")

if __name__ == "__main__":
    app = DataProcessingApp()
    app.run()
