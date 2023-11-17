import questionary
import real_time_data_processing
class DataProcessingApp:
    def __init__(self):
        self.menu_options = {
            "Procesamiento de Datos en Tiempo Real con Spark Streaming": self.real_time_data_processing,
            "Interacción con MongoDB usando PyMongoDB": self.mongodb_interaction,
            "Análisis de Datos con PySpark y Numpy": self.data_analysis,
            "Almacenamiento de Resultados en HDFS": self.hdfs_storage,
            "Visualización de Datos con PowerBI": self.data_visualization_powerbi,
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

    def real_time_data_processing(self):
        real_time_data_processing.main()

    def mongodb_interaction(self):
        print("Interactuando con MongoDB usando PyMongoDB...")

    def data_analysis(self):
        print("Analizando datos con PySpark y Numpy...")

    def hdfs_storage(self):
        print("Almacenando resultados en HDFS...")

    def data_visualization_powerbi(self):
        print("Visualizando datos con PowerBI...")

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
