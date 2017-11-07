# map-reduce

 - Descargar el proyecto:
		git clone https://github.com/fbartolome/map-reduce
 - Dirigirse a la carpeta tp
 - Ejecutar el script compile.sh
 - Dirigirse a la carpeta tp/server/target/hazelcast-client-1.0-SNAPSHOT/
 - Ejecutar el script run-server.sh [ip de un nodo]
 - Dirigirse a la carpeta tp/server/target/hazelcast-client-1.0-SNAPSHOT/
 - Ejecutar el script run-client.sh [args]
	 - Nota: Al pasar argumentos con espacios a run-client.sh este los separa erroneamente. Debe o correrse el comando:
		 - 'java -cp 'lib/jars/*' "ar.edu.itba.pod.client.Client" [args]'
	o reemplazar el script por
		- 'java -cp 'lib/jars/*' "ar.edu.itba.pod.client.Client" “$@”'
	
## Formato de argumentos en el server:
 - Daddress=x.x.x.x indica la dirección del nodo padre a las cual el server intentará conectarse.
## Formato de argumentos en el client:
 - Daddress=[x.x.x.x x.x.x.x…] indica las direcciones de ip a las cuales
   el cliente intentara conectarse. 
 - Dquery=x indica cual es el número de
   la query que se desea ejecutar. 
 - DinPath=x indica de donde se va a
   leer los datos del censo. 
 - DoutPath=x indica donde se escribirán los
   resultados. 
 - DtimeOutPath=x indica donde se escribirán los tiempos de
   ejecución. 
 - Dn=x argumento de query utilizado en las queries 2,6 y 7.
 - Dprov=x argumento de query utilizado en la query 2.

### Tener en cuenta que todos los paths deben ser absolutos.
					

