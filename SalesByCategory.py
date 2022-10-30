from mrjob.job import MRJob
from mrjob.step import MRStep

class SalesAnalsyer(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings)
        ]

    def mapper_get_sales(self, _, line):
        (event_time, event_type, product_id, category_id, category_code, brand, price, user_id) = line.split('\t')
        yield category_code, 1

    def reducer_count_sales(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    SalesAnalyser.run()
