import Fsensaciones
import Fcsv
import Fgenerales
import menu

menu.main()

candidatos = ['trump','biden']

for candidato in candidatos:
    Fcsv.limpiar_csv('hashtag_'+candidato+'.csv',candidato+'.csv')

sentimientos = Fgenerales.combinar(Fsensaciones.obtenerSensaciones(candidatos))
Fgenerales.convertirJSON(sentimientos,'Sentimientos')
