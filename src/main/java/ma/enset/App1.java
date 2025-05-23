package ma.enset;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import scala.Tuple2;
public class App1 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Total Ventes Par Ville");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lignes = sc.textFile("ventes.txt");
        JavaRDD<String> data = lignes;
        JavaPairRDD<String, Double> ventesVillePrix = data.mapToPair(ligne -> {
            String[] champs = ligne.trim().split("\\s+");
            String ville = champs[1];
            double prix = Double.parseDouble(champs[3]);
            return new Tuple2<>(ville, prix);
        });
        JavaPairRDD<String, Double> totalVentesParVille = ventesVillePrix.reduceByKey((a, b) -> a + b);
        totalVentesParVille.collect().forEach(tuple -> {
            System.out.println("Ville: " + tuple._1() + " - Total ventes: " + tuple._2());
        });
        sc.close();
    }
}