#include "core/graph.hpp"

using namespace std;

int main(int argc, char ** argv) {

    string graph_path = argv[1];
    long memory = 8l*1024l*1024l*1024l;

    if (argc >= 3)
        long memory_bytes = atol(argv[2])*1024l*1024l*1024l;

    Graph graph(graph_path);
    graph.set_memory_bytes(memory);

    BigVector<long> crossing(graph.path + "/crossing", graph.vertices);
    BigVector<long> degree(graph.path + "/degree", graph.vertices);
    BigVector<int>  color(graph.path + "/color", graph.vertices);

    graph.set_vertex_data_bytes( graph.vertices * sizeof(VertexId) );

    graph.stream_vertices<VertexId>([&](VertexId i){
        color[i] = (i&1) == 1? long(1) : long(0);       
        crossing[i] = 0;       
        degree[i] = 0;
        return 1;
    });

    graph.hint(color, crossing);
    graph.hint(color, degree);
    long crossing_n = 0;
    long degree_sum = 0;
	graph.stream_edges<float>(
		[&](Edge & e){
        if (color[e.source] != color[e.target]) {
		    write_add(&crossing[e.source], long(1));
        }
		write_add(&degree[e.source], long(1));
		return 0;
	    }, nullptr, 0, 1,
	[&](std::pair<VertexId,VertexId> source_vid_range){
		crossing.lock(source_vid_range.first, source_vid_range.second);
	    },
	[&](std::pair<VertexId,VertexId> source_vid_range){
		crossing.unlock(source_vid_range.first, source_vid_range.second);
	});

    degree_sum = 0;
    crossing_n = 0;
 	for (VertexId i=0;i<graph.vertices;i++) {
		degree_sum += degree[i];
        crossing_n += crossing[i];
	}

    printf("cross: %d \n", crossing_n);
    printf("graph edges: %d \n", graph.edges);
    double ans = double(crossing_n) / double(graph.edges);
    printf("conductance: %lf\n", ans);
    return 0;
}
