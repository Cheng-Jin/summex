package DataSources;

import java.util.EnumMap;

/**
 * Created by xdy on 2016/11/16.
 */
public class INFOS {
    public final static EnumMap<GRAPHS, Double> CLASSES = new EnumMap<>(GRAPHS.class);
    public final static EnumMap<GRAPHS, Double> PROPERTIES = new EnumMap(GRAPHS.class);
    public final static EnumMap<GRAPHS, Double> TRIPLES = new EnumMap<>(GRAPHS.class);

    static {
        CLASSES.put(GRAPHS.dogfood_graph_modified, 31773.0);
        CLASSES.put(GRAPHS.linkedmdb_graph, 694400.0);
        CLASSES.put(GRAPHS.geo_graph, 34959.0);
        CLASSES.put(GRAPHS.peel_graph, 76229.0);
        CLASSES.put(GRAPHS.myexperiment_graph, 528853.0);

        PROPERTIES.put(GRAPHS.dogfood_graph_modified, 275892.0);
        PROPERTIES.put(GRAPHS.linkedmdb_graph, 4550834.0);
        PROPERTIES.put(GRAPHS.geo_graph, 322219.0);
        PROPERTIES.put(GRAPHS.peel_graph, 195323.0);
        PROPERTIES.put(GRAPHS.myexperiment_graph, 2317561.0);

        TRIPLES.put(GRAPHS.dogfood_graph_modified, 304758.0);
        TRIPLES.put(GRAPHS.linkedmdb_graph, 4966905.0);
        TRIPLES.put(GRAPHS.geo_graph, 343733.0);
        TRIPLES.put(GRAPHS.peel_graph, 271369.0);
        TRIPLES.put(GRAPHS.myexperiment_graph, 2933943.0);
    }
}
