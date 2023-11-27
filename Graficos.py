import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# Lee el archivo JSON
df = pd.read_json('Sentimientos.json', lines=True)

# Asigna manualmente el valor promedio a 'Northern Mariana Islands'
df.loc[df['Estado'] == 'Northern Mariana Islands', 'Sentimiento'] = np.mean(df['Sentimiento'])

# Agrupa por 'state' y saca el promedio el soporte para cada estado
agrupado = df.groupby('Estado')['Sentimiento'].mean().reset_index()

print(agrupado)

# Extrae los datos en arrays
estados = agrupado['Estado'].values
promedios_soporte = agrupado['Sentimiento'].values

# Crea un gráfico de barras
colores = ['yellow' if valor < 0 else 'blue' for valor in promedios_soporte]

# Comentar linea dependiendo de el grafico que se quiero
plt.plot(estados, promedios_soporte)
# plt.bar(estados, promedios_soporte, color=colores)

# Ajusta la orientación de las etiquetas en el eje x
plt.xticks(rotation='vertical')

# Añade etiquetas al gráfico
plt.xlabel('Estados')
plt.ylabel('Promedio de Soporte')
plt.title('Promedio de Soporte por Estado en América')

print(estados)
print(promedios_soporte)

# Muestra el gráfico
plt.show()

