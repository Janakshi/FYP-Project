from kivy.app import App
from kivy.uix.widget import Widget
from kivy.lang import Builder
from kivy.core.window import Window
from kivy.properties import StringProperty
from kivy.properties import NumericProperty
import paho.mqtt.client as mqtt
import mysql.connector
import datetime
import xlsxwriter

Builder.load_file('gui.kv')


class MyGridLayout(Widget):
    gb1Angle = NumericProperty()
    b1AvaVol = StringProperty()
    gb1Pecnt = StringProperty()

    gb2Angle = NumericProperty()
    b2AvaVol = StringProperty()
    gb2Pecnt = StringProperty()

    gb3Angle = NumericProperty()
    b3AvaVol = StringProperty()
    gb3Pecnt = StringProperty()

    gb4Angle = NumericProperty()
    b4AvaVol = StringProperty()
    gb4Pecnt = StringProperty()

    gb5Angle = NumericProperty()
    b5AvaVol = StringProperty()
    gb5Pecnt = StringProperty()

    gb6Angle = NumericProperty()
    b6AvaVol = StringProperty()
    gb6Pecnt = StringProperty()


class IOTApp(App):
    client = mqtt.Client()

    def build(self):
        Window.size = (1600, 800)
        Window.clearcolor = (0 / 255, 0 / 255, 0 / 255, 1)

        self.client.on_message = self.onMessage
        self.client.connect('test.mosquitto.org', 1883)
        self.client.loop_start()
        self.client.subscribe("RBL/BPGP/#")

        return MyGridLayout()

    def createDB(self):
        mydb1 = mysql.connector.connect(
            host="localhost",
            user="Janu",
            password="MySQL123",
        )
        c = mydb1.cursor()
        now = datetime.datetime.now()
        month = now.strftime("%Y" + "%B")
        query = "CREATE DATABASE IF NOT EXISTS " + month
        c.execute(query)
        mydb1.commit()
        mydb1.close()

    def createTable(self, dbase):
        mydb1 = mysql.connector.connect(
            host="localhost",
            user="Janu",
            password="MySQL123",
            database=dbase
        )
        c = mydb1.cursor()
        now = datetime.datetime.now()
        query = "CREATE TABLE IF NOT EXISTS " + now.strftime("%Y" + "%B" + "%d") + " (Time VARCHAR(50), " \
                                                                                   "Booth1CurrentVolume VARCHAR(50), Booth1AddedVolume VARCHAR(50), " \
                                                                                   "Booth2CurrentVolume VARCHAR(50), Booth2AddedVolume VARCHAR(50), " \
                                                                                   "Booth3CurrentVolume VARCHAR(50), Booth3AddedVolume VARCHAR(50), " \
                                                                                   "Booth4CurrentVolume VARCHAR(50), Booth4AddedVolume VARCHAR(50), " \
                                                                                   "Booth5CurrentVolume VARCHAR(50), Booth5AddedVolume VARCHAR(50), " \
                                                                                   "Booth6CurrentVolume VARCHAR(50), Booth6AddedVolume VARCHAR(50))"
        c.execute(query)
        mydb1.commit()
        mydb1.close()

    def submit(self, dbase):
        mydb1 = mysql.connector.connect(
            host="localhost",
            user="Janu",
            password="MySQL123",
            database=dbase
        )
        c = mydb1.cursor()
        now = datetime.datetime.now()
        tm = now.strftime("%X")
        b1 = self.root.b1AvaVol
        b2 = self.root.b2AvaVol
        b3 = self.root.b3AvaVol
        b4 = self.root.b4AvaVol
        b5 = self.root.b5AvaVol
        b6 = self.root.b6AvaVol
        table = now.strftime("%Y"+"%B"+"%d")
        insert_stmt = "INSERT INTO " + table + "(Time, Booth1CurrentVolume, Booth1AddedVolume, Booth2CurrentVolume, Booth2AddedVolume, Booth3CurrentVolume, Booth3AddedVolume, Booth4CurrentVolume, Booth4AddedVolume, Booth5CurrentVolume, Booth5AddedVolume, Booth6CurrentVolume, Booth6AddedVolume) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        data = (tm, b1, self.addVol(dbase, table, "1", b1), b2, self.addVol(dbase, table, "2", b2), b3, self.addVol(dbase, table, "3", b3), b4, self.addVol(dbase, table, "4", b4), b5, self.addVol(dbase, table, "5", b5), b6, self.addVol(dbase, table, "6", b6),)
        c.execute(insert_stmt, data)
        mydb1.commit()
        mydb1.close()

    def addVol(self, dbase, table, booth, curVol):
        mydb1 = mysql.connector.connect(
            host="localhost",
            user="Janu",
            password="MySQL123",
            database=dbase
        )
        c = mydb1.cursor()
        try:
            curColumn = "Booth" + booth + "CurrentVolume"
            query = "SELECT " + curColumn + " FROM " + table + " ORDER BY Time DESC LIMIT 1"
            c.execute(query)
            preVol = c.fetchone()
            preVol = (preVol[0])

            curVol = int(curVol[:-1])
            preVol = int(preVol[:-1])

            if curVol > preVol:
                addVol = curVol - preVol
                return addVol
            else:
                return None

        except:
            return None

    def onMessage(self, client, userdata, msg):
        self.createDB()
        msg.payload = msg.payload.decode("utf-8")

        if msg.topic == "RBL/BPGP/Glaze booth 1 level :":
            self.root.gb1Level = float(msg.payload)
            self.root.gb1Angle = round((560 - self.root.gb1Level) / 400 * 360)
            self.root.b1AvaVol = str(round((560 - self.root.gb1Level) * 0.6)) + "L"
            self.root.gb1Pecnt = str(round(self.root.gb1Angle / 360 * 100)) + "%"

        elif msg.topic == "RBL/BPGP/Glaze booth 2 level :":
            self.root.gb2Level = float(msg.payload)
            self.root.gb2Angle = round((560 - self.root.gb2Level) / 400 * 360)
            self.root.b2AvaVol = str(round((560 - self.root.gb2Level) * 0.6)) + "L"
            self.root.gb2Pecnt = str(round(self.root.gb2Angle / 360 * 100)) + "%"

        elif msg.topic == "RBL/BPGP/Glaze booth 3 level :":
            self.root.gb3Level = float(msg.payload)
            self.root.gb3Angle = round((560 - self.root.gb3Level) / 400 * 360)
            self.root.b3AvaVol = str(round((560 - self.root.gb3Level) * 0.6)) + "L"
            self.root.gb3Pecnt = str(round(self.root.gb3Angle / 360 * 100)) + "%"

        elif msg.topic == "RBL/BPGP/Glaze booth 4 level :":
            self.root.gb4Level = float(msg.payload)
            self.root.gb4Angle = round((560 - self.root.gb4Level) / 400 * 360)
            self.root.b4AvaVol = str(round((560 - self.root.gb4Level) * 0.6)) + "L"
            self.root.gb4Pecnt = str(round(self.root.gb4Angle / 360 * 100)) + "%"

        elif msg.topic == "RBL/BPGP/Glaze booth 5 level :":
            self.root.gb5Level = float(msg.payload)
            self.root.gb5Angle = round((560 - self.root.gb5Level) / 400 * 360)
            self.root.b5AvaVol = str(round((560 - self.root.gb5Level) * 0.6)) + "L"
            self.root.gb5Pecnt = str(round(self.root.gb5Angle / 360 * 100)) + "%"

        elif msg.topic == "RBL/BPGP/Glaze booth 6 level :":
            self.root.gb6Level = float(msg.payload)
            self.root.gb6Angle = round((560 - self.root.gb6Level) / 400 * 360)
            self.root.b6AvaVol = str(round((560 - self.root.gb6Level) * 0.6)) + "L"
            self.root.gb6Pecnt = str(round(self.root.gb6Angle / 360 * 100)) + "%"
            now = datetime.datetime.now()
            month = now.strftime("%Y" + "%B")
            self.createTable(month)
            self.submit(month)

    def fetch_table_data(self, table_name):
        mydb1 = mysql.connector.connect(
            host="localhost",
            user="Janu",
            password="MySQL123",
            database="maindb"
        )

        c = mydb1.cursor()
        c.execute('select * from ' + table_name)

        header = [row[0] for row in c.description]

        rows = c.fetchall()

        mydb1.close()

        return header, rows

    def generateToExcel(self, table_name):
        workbook = xlsxwriter.Workbook(table_name + '.xlsx')
        worksheet = workbook.add_worksheet('MENU')

        header_cell_format = workbook.add_format({'bold': True, 'border': True, 'bg_color': 'yellow'})
        body_cell_format = workbook.add_format({'border': True})

        header, rows = self.fetch_table_data(table_name)

        row_index = 0
        column_index = 0

        for column_name in header:
            worksheet.write(row_index, column_index, column_name, header_cell_format)
            column_index += 1

        row_index += 1
        for row in rows:
            column_index = 0
            for column in row:
                worksheet.write(row_index, column_index, column, body_cell_format)
                column_index += 1
            row_index += 1

        print(str(row_index) + ' rows written successfully to ' + workbook.filename)

        workbook.close()


if __name__ == '__main__':
    IOTApp().run()
