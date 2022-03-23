from datetime import datetime
import copy
import math
EXPIRE_LIMIT = 5

class bellmanFord(object):

    def __init__(self):
        self.currencies = {}

    def add_edge_helper(self, from_vertex, to_vertex, rate, timestamp):
        if from_vertex != to_vertex:
            if from_vertex not in self.currencies:
                self.currencies[from_vertex] = {}
                #self.currencies[from_vertex][to_vertex] = {}

            if to_vertex not in self.currencies[from_vertex]:
                #self.currencies[to_vertex] = {}
                self.currencies[from_vertex][to_vertex] = {}

            if self.currencies[from_vertex][to_vertex] != {}:
                if self.currencies[from_vertex][to_vertex][1] > timestamp:
                    print('ignoring out-of-sequence message')
                    return

            self.currencies[from_vertex][to_vertex] = (float(rate), timestamp)

    def add_edge(self, edge):
        currency = edge['cross']
        cross = currency.split('/')
        rate = -math.log10(float(edge['price']))
        reverseRate = math.log10(float(edge['price']))
        from_vertex = cross[0]
        to_vertex = cross[1]
        timestamp = edge['timestamp']

        self.add_edge_helper(from_vertex, to_vertex, rate, timestamp)
        # add the reverse edge
        self.add_edge_helper(to_vertex, from_vertex, reverseRate, timestamp)

    def removeExpiredEntry(self):
        temp = copy.deepcopy(self.currencies)
        for key, value in temp.items():
            for subKey, subValue in value.items():
                if (datetime.utcnow() - temp[key][subKey][1]).total_seconds() > EXPIRE_LIMIT:
                    if self.currencies[key]:
                        print('removing stale quote for ({}, {})'.format(key, subKey))
                        self.currencies[key].pop(subKey, None)
                    if self.currencies[subKey]:
                        print('removing stale quote for ({}, {})'.format(subKey, key))
                        self.currencies[subKey].pop(key, None)

    def bellman_ford(self, start_vertex):
        self.removeExpiredEntry()
        predecessor = {}
        graph = {}

        #currencies = self.preprocess()

        for u in self.currencies:
            graph[u] = float('inf')
            predecessor[u] = None

        graph[start_vertex] = 0

        for _ in range(len(graph)-1):
            for node in graph:
                for neighbour in self.currencies[node]:
                    weight = self.currencies[node][neighbour][0]
                    if graph[neighbour] > graph[node] + weight:
                        graph[neighbour] = graph[node] + weight
                        predecessor[neighbour] = node

        node = start_vertex
        for neighbour in self.currencies[node]:
            weight = self.currencies[node][neighbour][0]
            if graph[neighbour] > graph[node] + weight:
                print("Graph contains negative cycle")
                list = traceback(predecessor, start_vertex)
                list = list[::-1]

                if list[0] != start_vertex:
                    print('no negative cycle')
                    return None
                print(list)
                if len(list) <= 3:
                    return None
                return list
                #return destination, predecessor
        print('no negative cycle')

        return None



def traceback(predecessor, node):
    result = [node]
    nextNode = node

    while True:
        nextNode = predecessor[nextNode]
        if nextNode not in result:
            result.append(nextNode)
        else:
            result.append(nextNode)
            result = result[result.index(nextNode):]
            return result