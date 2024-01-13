package consultas;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @Description Clase Mapper para el análisis de ventas por mes. Esta clase extiende
 * MapReduceBase e implementa la interfaz Mapper de Hadoop. Su función principal es procesar
 * las líneas de texto (registros de ventas) y emitir el mes y el año como clave y el monto total
 * de ventas como valor.
 * @author Jose Luis Obiang Ela Nanguang
 * @version 1.0
 * {@link MapReduceBase}
 * {@link Mapper}
 */

public class VentasPorMesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

    private static final Map<String, String> meses = new HashMap<>();

    static {
        // Inicializa el mapa con los nombres de los meses.
        meses.put("01", "Enero");
        meses.put("02", "Febrero");
        meses.put("03", "Marzo");
        meses.put("04", "Abril");
        meses.put("05", "Mayo");
        meses.put("06", "Junio");
        meses.put("07", "Julio");
        meses.put("08", "Agosto");
        meses.put("09", "Septiembre");
        meses.put("10", "Octubre");
        meses.put("11", "Noviembre");
        meses.put("12", "Diciembre");
    }

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
        // Divide la línea de entrada y verifica que tenga al menos 6 campos.
        String line = value.toString();
        String[] fields = line.split(",");

        if (fields.length > 5) {
            // Extrae y procesa la fecha para obtener el mes y el año.
            String date = fields[4];
            String[] dateParts = date.split(" ")[0].split("/");
            if (dateParts.length == 3) {
                // Construye la clave compuesta por el nombre del mes y el año.
                String monthYear = meses.get(dateParts[0]) + "(" + dateParts[0] + ")/20" + dateParts[2];

                try {
                    // Calcula el total de ventas, multiplicando la cantidad de ventas por precio
                    int numVentas = Integer.parseInt(fields[2]); // Extraer la cantidad de ventas
                    double precio = Double.parseDouble(fields[3]); // Extraer el precio

                    double gananciaPorVenta = numVentas * precio; // Calcula las ganancias por venta
                    // Formatea la salida para alinear adecuadamente según el mes.
                    if (dateParts[0].equals("04") || dateParts[0].equals("08") || dateParts[0].equals("01") || dateParts[0].equals("07") || dateParts[0].equals("06") || dateParts[0].equals("03") || dateParts[0].equals("05") || dateParts[0].equals("02")){
                        output.collect(new Text(monthYear + "     \t\t"), new DoubleWritable(gananciaPorVenta));
                    } else{
                        output.collect(new Text(monthYear + "\t\t"), new DoubleWritable(gananciaPorVenta));
                    }

                } catch (NumberFormatException e) {
                    // Registra errores si los datos de ventas no son numéricos.
                    reporter.setStatus("Error al procesar la línea: '" + line + "'. Asegúrate de que la cantidad y el precio sean numéricos.");
                }
            }
        }
    }
}
