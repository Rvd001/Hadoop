from mrjob.job import MRJob

# Running using HDFS & Hadoop: python3.5 wordCount.py -r hadoop hdfs:///Data/Words.txt
# NOTE the extra / in the path for the HDFS file
class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
        yield "chars", len(line)
        yield "words", len(line.split())
        yield "lines", 1

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MRWordFrequencyCount.run()
