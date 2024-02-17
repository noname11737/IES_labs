from csv import reader
from datetime import datetime
from domain.aggreg import AggData

from domain.gps import Gps
from domain.accel import Accel

class DataSource:
    def __init__(self,files:list)->None:
        #don`t need to hold files opened
        self.data = {}
        for file_data in files:
                name = file_data['name'].split('/')[-1].split('.')[0]
                self.data[name] = {"iter":0,"data":[]}
                with open(file_data["name"],'r') as csv_f:
                    rdr = reader(csv_f,delimiter=",")
                    for row in rdr:
                        res = self.__parse_line(row,file_data["struct"])
                        if res is not None:
                            self.data[name]["data"].append(res)
        #print(self.data)
        return None



    @staticmethod
    def __parse_line(line:list,line_struct:list)->list:
        try:
            return [line_struct[ind](line[ind]) for ind,unit in enumerate(line)]
        except Exception:
            return None
        
    def read(self)->AggData:
        res = AggData(accel=Accel(x=self.data["accelerometer"]["data"][self.data["accelerometer"]["iter"]][0],
                                    y=self.data["accelerometer"]["data"][self.data["accelerometer"]["iter"]][1],
                                    z=self.data["accelerometer"]["data"][self.data["accelerometer"]["iter"]][2]),
                        gps=Gps(long=self.data["gps"]["data"][self.data["gps"]["iter"]][0],
                                lat=self.data["gps"]["data"][self.data["gps"]["iter"]][1]),
                                time=datetime.now())
        for k in self.data.keys():
            self.data[k]["iter"]+=1
            if self.data[k]["iter"]==len(self.data[k]["data"]):
                self.data[k]["iter"]=0
        return res

    def start_r(self,*args,**kwargs):
        pass


    def stop_r(self,*args,**kwargs):
        for i,_ in enumerate(self.data):
            match self.method:
                case 0:
                    for i,_ in enumerate(self.data):
                        self.data[i]["data"].clear()
                        self.data[i]["iter"]=0
                case 1:
                    for i,_ in enumerate(self.data):
                        self.data[i]["struct"].clear()
                        self.data[i]["ptr"].close()
                    

if __name__ == "__main__":
    src = DataSource([{"name":"./src/data/accelerometer.csv","struct":[int,int,int]},
                      {"name":"./src/data/gps.csv","struct":[float,float]}])
