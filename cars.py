import hashlib

from pymongo import MongoClient
from pymongo import errors


class AbstractBaseCar:

    DEBUG = True

    car_details = {
        "url": "",
        "name": "",
        "price": "",
        "brand": "",
        "modell": "",
        "km_driven": "",
        "zulassung": "",
        "ps": "",
        "farbe": "",
        "getriebe": "",
        "hubraum": "",
        "zylinder": "",
        "kraftstoff": "",
        "sha1": "",
        "isInvalid": False,
    }

    def __get_info__(self, response):
        """
        The actual parsing has to done in this function.
        :param response:
        :return:
        """
        pass

    def __check_if_valid__(self):
        """
        Check if all fields have proper values.
        :return:
        """
        pass

    def _createsha1(self):

        """
        Creates a sha1 hash of this particular car object to avoid duplicates in the dataset.
        :return: sha1 hexdigest
        """

        hash = hashlib.sha1()
        hash_object = (
                self.car_details["url"] +
                self.car_details["name"] +
                self.car_details["brand"] +
                self.car_details["modell"] +
                self.car_details["price"] +
                self.car_details["km_driven"] +
                self.car_details["zulassung"] +
                self.car_details["ps"] +
                self.car_details["farbe"] +
                self.car_details["getriebe"] +
                self.car_details["hubraum"] +
                self.car_details["kraftstoff"]
        )
        hash.update(hash_object.encode('utf-8'))
        return hash.hexdigest()

    def _feeddatabase(self):

        """
        Feed Data to MongoDB.
        We feed a shallow copy to MogoDB to avoid that the same _id_ gets assigned to all objects.

        Is it better to create a new connection for each object and store it individually or collect all
        objects in memory and bulk feed them?

        The sha1 field shall be the unique Index to avoid duplicates. The database was created with:
        db.cars.createIndex( { "sha1": 1 }, { unique: true, sparse: true } )

        """

        self.__check_if_valid__()
        if self.car_details["isInvalid"] == False:
            client = MongoClient('localhost', 27017)
            db = client.cars
            try:
                this_car = db.cars.insert_one(self.car_details.copy()).inserted_id
            except errors.DuplicateKeyError:
                print("Could not insert key: {} \nKey already in Database!\n".format(self.car_details["sha1"]))
            finally:
                client.close()

    def __init__(self, response):

        self.__get_info__(response)

    def _print_debug(self):

        """
        Some debug info to check if all the values are correctly parsed and sanitized
        TODO: remove?
        :return:
        """

        print("Url : " + self.car_details["url"])
        print("Name : " + self.car_details["name"])
        print("Brand : " + self.car_details["brand"])
        print("Modell : " + self.car_details["modell"])
        print("Preis : " + self.car_details["price"])
        print("KM : " + self.car_details["km_driven"])
        print("Zulassung : " + self.car_details["zulassung"])
        print("PS : " + self.car_details["ps"])
        print("Farbe : " + self.car_details["farbe"])
        print("Getriebe : " + self.car_details["getriebe"])
        print("Hubraum : " + self.car_details["hubraum"])
        print("Zylinder : " + self.car_details["zylinder"])
        print("Kraftstoff : " + self.car_details["kraftstoff"])
        print("SHA1 : " + str(self.car_details["sha1"]))
        print("Is_Invalid : " + str(self.car_details["isInvalid"]))
        print("\n")


class AutoscoutCar(AbstractBaseCar):

    def __get_info__(self, response):

        """
        The parser, Here is where the magic happens. Read and
        sanitize the results and assign the according values.

        TODO: Scrapy Fields &? ItemPipeline or handle in Class

        After assigning all values they are fed into the database

        :param response: (scrapy request)
        :return: NIL
        """
        try:
            name_string = str(response.xpath('/html/body/div[1]/main/div[2]/div/div[1]/h1/span[1]').extract())\
                .split(" ")
            self.car_details["url"] = response.url
            self.car_details["brand"] = name_string[2][13:]
            self.car_details["modell"] = name_string[3][:-10]
            self.car_details["price"] = \
                str(response.xpath('/html/body/div[1]/main/div[3]/div[3]/div[1]/div[1]/h2').extract()).split(" ")[1][
                :-11].replace(".", "")
            self.car_details["km_driven"] = \
                str(response.xpath('/html/body/div[1]/main/div[3]/div[3]/div[2]/div/div[1]/span[1]').extract()).split('>')[
                    1].split(' ')[0].replace(".", "")
            self.car_details["zulassung"] = \
                str(response.xpath('/html/body/div[1]/main/div[3]/div[3]/div[2]/div/div[2]/span[1]').extract()).split('>')[
                    1][
                :-6]
            self.car_details["ps"] = \
                str(response.xpath('/html/body/div[1]/main/div[3]/div[3]/div[2]/div[1]/div[3]/span[2]').extract()).split(
                    '>')[
                    1][:-9]
            self.car_details["farbe"] = str(response.xpath(
                '/html/body/div[1]/main/div[4]/div/div/div[7]/div[2]/div[1]/div[2]/dl/dd[2]').extract()).split('\\')[1][1::]
            self.car_details["getriebe"] = str(response.xpath(
                '/html/body/div[1]/main/div[4]/div/div/div[7]/div[2]/div[1]/div[3]/dl/dd[1]').extract()).split('\\')[1][1::]
            self.car_details["kraftstoff"] = str(response.xpath(
                '/html/body/div[1]/main/div[4]/div/div/div[7]/div[2]/div[2]/div/div/div/div[1]/dl/dd[1]').extract()).split(
                '\\')[1][1::]
            self._get_dynamic(response)
            self.car_details["name"] = self._get_name(response)
            self.car_details["sha1"] = self._createsha1()
        except:
            if self.DEBUG:
                self._print_debug()
            else:
                print("Error")

        self._feeddatabase()

    def _get_name(self, response):
        full_name = self.car_details['brand'] + " " + self.car_details['modell']
        try:
            full_name = full_name + response.xpath('/html/body/div[1]/main/div[2]/div/div[1]/h1/span[2]').split('>')[1][:-6]
        except:
            pass

        return full_name

    def _get_dynamic(self, response):

        """
        There are a few xpath which seem to vary across different sites. This function sanitizes the input and assigns
        the correct value to each field. Both x-paths can either be the Hubraum or the Zylinder.

        The value of Zylinder can not be greater than 10 so a hacky fix.

        :param response: scrapy.response
        :return:
        """

        zyl_or_hubraum = str(response.xpath(
            '/html/body/div[1]/main/div[4]/div/div/div[7]/div[2]/div[1]/div[3]/dl/dd[3]').extract()).split('\\')[1][
                         1::].split(' ')[0].replace('.', '')

        try:
            if int(zyl_or_hubraum) > 10:
                self.car_details["hubraum"] = zyl_or_hubraum
                tst = str(response.xpath(
                    '/html/body/div[1]/main/div[4]/div/div/div[7]/div[2]/div[1]/div[3]/dl/dd[2]').extract()).split(
                    '\\')[1][
                      1::].split()[0].replace(".", "")
                if int(tst) > 10:
                    self.car_details["isInvalid"] = True
                else:
                    self.car_details["zylinder"] = tst

            else:
                self.car_details["zylinder"] = zyl_or_hubraum
                self.car_details["hubraum"] = str(response.xpath(
                    '/html/body/div[1]/main/div[4]/div/div/div[7]/div[2]/div[1]/div[3]/dl/dd[2]').extract()).split(
                    '\\')[1][
                                              1::].split()[0].replace(".", "")
        except ValueError:
            """ 
            In rare cases the field can be a random string. In such cases we disregard the entire entry. Due to threading
            issues the check can not be reliably performed here and needs to be repeated before feeding the Database. 

            TODO: Find a fix!
            """

            self.car_details["isInvalid"] = True

    def __check_if_valid__(self):

        try:
            if int(self.car_details["zylinder"]) < 10 and int(self.car_details["hubraum"]) > 100:
                self.car_details["isInvalid"] = False
            else:
                self.car_details["isInvalid"] = True

        except ValueError:
            self.car_details["isInvalid"] = True

        if self.car_details["modell"] == "":
            self.car_details["isInvalid"] = True

        pass
