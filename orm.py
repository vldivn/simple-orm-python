import sys
import psycopg2
import psycopg2.extras
import json

from config import conn_string


class Orm:
    """A simple orm"""

    def __init__(self):
        try:
            self.connection = psycopg2.connect(conn_string())
            self.cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        except:
            print "I am unable to connect to the database."

    def table_name(self):
        pass

    def find(self, id = None):
        try:
            if(id != None):
                self.cursor.execute("""SELECT * from %s WHERE id = %i""" % (self.table_name(), id))
            else:
                self.cursor.execute("""SELECT * from %s""" % (self.table_name()))
            return self
        except:
            print "Cursor execute exception, from table %s" % self.table_name()

    def _insert(self):
        keys = dict()
        for key, value in self.attrs.items():
            if key != 'id':
                keys.update({key: value})
        try:
            self.cursor.execute("""INSERT INTO %s (%s) VALUES (%s)""" % (self.table_name(), ', '.join("%s" % key for key, value in keys.items()), ', '.join("'%s'" % value for key, value in keys.items())))
            self.connection.commit()
        except:
            print "Unexpected error:", sys.exc_info()[0]


    def _update(self):
        keys = dict()
        for key, value in self.attrs.items():
            if key != 'id':
                keys.update({key: value})
        try:
            self.cursor.execute("""UPDATE %s SET %s WHERE id = %s""" % (self.table_name(), ', '.join("%s = '%s'" % (key, value) for key, value in keys.items()), self.attrs['id']))
            self.connection.commit()
        except:
            print "Unexpected error:", sys.exc_info()[0]

    def delete(self):
        try:
            self.cursor.execute("""DELETE FROM %s WHERE id = %s""" % (self.table_name(), self.attrs['id']))
            self.connection.commit()
        except:
            print "Unexpected error:", sys.exc_info()[0]


class Video(Orm):
    """Video class
    title
    description
    """

    def __init__(self, new_record = True):
        Orm.__init__(self)
        self.attrs = dict()
        self.is_new_record = new_record

    def table_name(self):
        return 'public.films'

    def all(self):
        tmp = []
        if self.cursor:
            for row in self.cursor:
                tmp.append(Video.parse_dict(row))
        else:
            return None

        return tmp

    def one(self):
        if self.cursor:
            return Video.parse_dict(self.cursor.fetchone())
        else:
            return None

    def parse_dict(dict):
        if (dict):
            tmp = Video()
            tmp.is_new_record = False
            for k, v in dict.iteritems():
                tmp.attrs.update({k: v})
            return tmp
        else:
            return None

    parse_dict = staticmethod(parse_dict)

    def __str__(self):
        return self.attrs.__str__()

    def save(self):
        if self.is_new_record:
            self._insert()
        else:
            self._update()

#video = Video().find().one()

video = Video().find(id = 1).one()
print video
video.delete()
#video = Video().find(id = 1).one()
#print video

#video = Video().find().one()
#video.attrs.update({'title': 'Mask 2'})
#video.save()

#video = Video()
#video.attrs.update({'title': 'Star Wars'})
#video.attrs.update({'description': "Im your father!"})
#video.save()
