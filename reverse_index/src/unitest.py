import pyspark
import re
import unittest


class PySparkTestCase(unittest.TestCase):

    def setUpClass(self, cls):
        conf = pyspark.SparkConf()
        cls.sc = pyspark.SparkContext(conf=conf)
        cls.spark = pyspark.SQLContext(cls.sc)

    def tearDownClass(self, cls):
        cls.sc.stop()


class basicTest(PySparkTestCase):

    def normalizeWords(self, text):
        return re.compile(r'\W+', re.UNICODE).split(text.lower())

    def filename_value(self, word_set_file):
        word_set, file = word_set_file
        try:
            filename = int(file.split("/")[-1])
            tup = ()
            for word in word_set:
                if word != "":
                    tup += ((word, [filename]),)
            return tup
        except:
            print('filename not int')

    def test_clean(self):
        test_input = [('path/name', 'Test inPut!\n#1')]
        test_rdd = self.sc.parallelize(test_input).map(self.normalizeWords)
        self.assertEqual(test_rdd.collect(), [
                         ({'1', 'input', 'test'}, 'path/name')])

    def test_filename_value(self):
        test_input = [({'1', 'input', 'test'}, 'path/name/1')]
        test_rdd = self.sc.parallelize(test_input).map(self.filename_value)
        self.assertEqual(test_rdd.collect(), [
                         (('input', [1]), ('1', [1]), ('test', [1]))])


if __name__ == "__main__":
    unittest.main()
