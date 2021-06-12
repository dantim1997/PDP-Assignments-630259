from mrjob.job import MRJob
from mrjob.step import MRStep

class RatingCount(MRJob):
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                reducer=self.reducer
            ),
            MRStep(
                reducer=self.sorting
            )
        ]

    def mapper(self, _, line):
        (user_id, movie_id, rating, rating_time) = line.split('\t')
        yield int(movie_id), int(rating)

    def reducer(self, movie_id, ratings):
        yield None, (len(list(ratings)), movie_id)

    def sorting(self, _, countPairs):
        for count, movie_id in sorted(countPairs, reverse=True):
            yield movie_id, count

if __name__ == '__main__':
    RatingCount.run()

