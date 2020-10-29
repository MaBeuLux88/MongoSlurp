import unittest
from datetime import datetime

from bson import ObjectId

from mongo_slurp import guess_type, guess_types_and_values, create_final_documents, parse


class MyTestCase(unittest.TestCase):

    def test_guess_type(self):
        self.assertEqual(guess_type(123), 'Integer')
        self.assertEqual(guess_type('123'), 'Integer')
        self.assertEqual(guess_type('01234'), 'String')
        self.assertEqual(guess_type('123.456'), 'Double')
        self.assertEqual(guess_type(123.456), 'Double')
        self.assertEqual(guess_type(123.0), 'Double')
        self.assertEqual(guess_type('123,456'), 'Double')
        self.assertEqual(guess_type('123,456'), 'Double')
        self.assertEqual(guess_type('123string123'), 'String')
        self.assertEqual(guess_type('string'), 'String')
        self.assertEqual(guess_type('true'), 'Boolean')
        self.assertEqual(guess_type('false'), 'Boolean')
        self.assertEqual(guess_type('ObjectId(5c8eccc1caa187d17ca6ed16)'), 'ObjectId')
        self.assertEqual(guess_type('5c8eccc1caa187d17ca6ed16'), 'ObjectId')
        self.assertEqual(guess_type('5c8eccc1caa187d17ca6ed1'), 'String')
        self.assertEqual(guess_type('Salut true not a boolean'), 'String')
        self.assertEqual(guess_type('Salut 12.3 not a Double'), 'String')
        self.assertEqual(guess_type('2020-10-10'), 'Date')
        self.assertEqual(guess_type('2020/10/10'), 'Date')
        self.assertEqual(guess_type('2020-1-1'), 'Date')
        self.assertEqual(guess_type('2020/1/1'), 'Date')
        self.assertEqual(guess_type('20-10-2020'), 'Date')
        self.assertEqual(guess_type('20/10/2020'), 'Date')
        self.assertEqual(guess_type('2-1-2020'), 'Date')
        self.assertEqual(guess_type('1979-09-24T18:05:07'), 'Date')
        self.assertEqual(guess_type('1979-09-24R18:05:07'), 'String')
        self.assertEqual(guess_type('2020-10-2020'), 'String')
        self.assertEqual(guess_type('2020/10/2020'), 'String')

    def test_guess_types_and_values(self):
        self.assertEqual(guess_types_and_values({'a': 'string', 'b': 123}), [('a', 'String', "doc['a']"), ('b', 'Integer', "doc['b']")])
        self.assertEqual(guess_types_and_values({'a': '2020-10-1', 'b': 123.21}), [('a', 'Date', "doc['a']"), ('b', 'Double', "doc['b']")])
        self.assertEqual(guess_types_and_values({'a': '2020-10-1', 'b': '123.21'}), [('a', 'Date', "doc['a']"), ('b', 'Double', "doc['b']")])
        self.assertEqual(
            guess_types_and_values(
                {'_id': '5f97134bec4357f333afd3ff', 'firstname': 'Jennifer', 'lastname': 'Carter', 'age': 75, 'size': 191.55, 'alive': 'false', 'timestamp': '1105926688',
                 'date_of_birth': '1989-01-22', 'licence_date': '7-08-1998', 'certificate_date': '1979-09-24T18:05:07', 'year_inserted': 1981, 'month_inserted': 10,
                 'day_inserted': 25, 'address_number': 97630, 'street_name': 'Berger Plain', 'postcode': '04165', 'city': 'Jillchester', 'country': 'Mozambique'}),
            [('_id', 'ObjectId', "doc['_id']"), ('firstname', 'String', "doc['firstname']"), ('lastname', 'String', "doc['lastname']"), ('age', 'Integer', "doc['age']"),
             ('size', 'Double', "doc['size']"), ('alive', 'Boolean', "doc['alive']"), ('timestamp', 'Integer', "doc['timestamp']"),
             ('date_of_birth', 'Date', "doc['date_of_birth']"), ('licence_date', 'Date', "doc['licence_date']"), ('certificate_date', 'Date', "doc['certificate_date']"),
             ('year_inserted', 'Integer', "doc['year_inserted']"), ('month_inserted', 'Integer', "doc['month_inserted']"), ('day_inserted', 'Integer', "doc['day_inserted']"),
             ('address_number', 'Integer', "doc['address_number']"), ('street_name', 'String', "doc['street_name']"), ('postcode', 'String', "doc['postcode']"),
             ('city', 'String', "doc['city']"), ('country', 'String', "doc['country']")])
        self.assertEqual(
            guess_types_and_values(
                {'_id': 'ObjectId(5c8eccc1caa187d17ca6ed16)', 'city': 'ALPINE', 'zip': 35014, 'loc.x': 86.208934, 'loc.y': 33.331165, 'pop': 3062, 'state': 'AL'}),
            [('_id', 'ObjectId', "doc['_id']"), ('city', 'String', "doc['city']"), ('zip', 'Integer', "doc['zip']"), ('loc.x', 'Double', "doc['loc.x']"),
             ('loc.y', 'Double', "doc['loc.y']"), ('pop', 'Integer', "doc['pop']"), ('state', 'String', "doc['state']")])

    def test_create_final_documents(self):
        self.assertEqual(create_final_documents([{'a': 12}], [('a', 'Integer', "doc['a']")]), [{'a': 12}])
        self.assertEqual(create_final_documents([{'a': "12"}], [('a', 'Integer', "doc['a']")]), [{'a': 12}])
        self.assertEqual(create_final_documents([{'a': "12"}], [('a', 'String', "doc['a']")]), [{'a': '12'}])
        self.assertEqual(create_final_documents([{'loc.x': 12}], [('loc.x', 'Integer', "doc['loc.x']")]), [{'loc': {'x': 12}}])
        self.assertEqual(create_final_documents([{'a.b.c.d': 1}], [('a.b.c.d', 'Integer', "doc['a.b.c.d']")]), [{'a': {'b': {'c': {'d': 1}}}}])
        self.assertEqual(create_final_documents([{}], [('a', 'String', 'Point')]), [{'a': 'Point'}])

    def test_parse(self):
        self.assertEqual(parse('12', 'Integer'), 12)
        self.assertEqual(parse('12', 'String'), '12')
        self.assertEqual(parse('true', 'Boolean'), True)
        self.assertEqual(parse('TRUE', 'Boolean'), True)
        self.assertEqual(parse('TrUe', 'Boolean'), True)
        self.assertEqual(parse('false', 'Boolean'), False)
        self.assertEqual(parse('0', 'Boolean'), False)
        self.assertEqual(parse('true', 'String'), 'true')
        self.assertAlmostEqual(parse('1.2', 'Double'), 1.2)
        self.assertAlmostEqual(parse('2.0', 'Double'), 2.0)
        self.assertEqual(parse('2.0', 'String'), '2.0')
        self.assertEqual(parse('ObjectId(5c8eccc1caa187d17ca6ed16)', 'ObjectId'), ObjectId('5c8eccc1caa187d17ca6ed16'))
        self.assertEqual(parse('5c8eccc1caa187d17ca6ed16', 'ObjectId'), ObjectId('5c8eccc1caa187d17ca6ed16'))
        self.assertEqual(parse('5c8eccc1caa17d17ca6ed16', 'ObjectId'), "5c8eccc1caa17d17ca6ed16")
        self.assertEqual(parse('2020-10-10', 'Date'), datetime(2020, 10, 10))
        self.assertEqual(parse('2020/10/10', 'Date'), datetime(2020, 10, 10))
        self.assertEqual(parse('2020-1-1', 'Date'), datetime(2020, 1, 1))
        self.assertEqual(parse('2020/1/1', 'Date'), datetime(2020, 1, 1))
        self.assertEqual(parse('20-10-2020', 'Date'), datetime(2020, 10, 20))
        self.assertEqual(parse('20/10/2020', 'Date'), datetime(2020, 10, 20))
        self.assertEqual(parse('2-1-2020', 'Date'), datetime(2020, 2, 1))
        self.assertEqual(parse('1979-09-24T18:05:07', 'Date'), datetime(1979, 9, 24, 18, 5, 7))
        self.assertEqual(parse('1105926688', 'Date'), datetime(2005, 1, 17, 2, 51, 28))
        self.assertEqual(parse('1979-09-24R18:05:07', 'String'), '1979-09-24R18:05:07')
        self.assertEqual(parse('2020-10-2020', 'String'), '2020-10-2020')
        self.assertEqual(parse('2020/10/2020', 'String'), '2020/10/2020')


if __name__ == '__main__':
    unittest.main()
