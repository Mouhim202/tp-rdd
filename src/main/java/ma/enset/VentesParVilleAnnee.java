package ma.enset;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import scala.Tuple2;

public class VentesParVilleAnnee {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("Calcul des ventes par ville et par année").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lignes = sc.textFile("ventes.txt");

        JavaPairRDD<Tuple2<String, String>, Double> ventesVilleAnnee = lignes.mapToPair(ligne -> {
            String[] champs = ligne.trim().split("\\s+");
            String date = champs[0];
            String ville = champs[1];
            double prix = Double.parseDouble(champs[3]);
            String annee = date.split("-")[0];
            return new Tuple2<>(new Tuple2<>(ville, annee), prix);
        });

        JavaPairRDD<Tuple2<String, String>, Double> totalParVilleAnnee = ventesVilleAnnee.reduceByKey((a, b) -> a + b);

        totalParVilleAnnee.collect().forEach(tuple -> {
            String ville = tuple._1()._1();
            String annee = tuple._1()._2();
            Double total = tuple._2();
            System.out.println("Ville : " + ville + " | Année : " + annee + " | Total des ventes : " + total);
        });

        sc.close();
    }
}
