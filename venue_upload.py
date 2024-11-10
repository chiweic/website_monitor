# extract all venue from website and upload
# pretty simple web page, so avoid two stage crawl/parsing

import requests # type: ignore
import logging
from bs4 import BeautifulSoup
import json
import os
#from supabase import create_client, Client
#from dotenv import load_dotenv


#load_dotenv()

#url: str = os.environ.get("SUPABASE_URL")
#key: str = os.environ.get("SUPABASE_KEY")
#supabase: Client = create_client(url, key)


field_mapping = {'電話':'Tel', '地址':'Address', '傳真':'Fax', '信箱':'Mail'}
region_mapping = {'1': 'tw', '2': 'overseas'}

def download_venues(region=1):
    
    # tw:1 oversea:2
    # set up to crawl all venues from main website
    root_url = 'https://www.ddm.org.tw/xclocation?xsmsid=0K296464423399846372&region={}'.format(region)
    # LocationList tw
    soup = BeautifulSoup(requests.get(root_url).text, "html.parser")

    # tw or oversea
    if region == 1:
        elem = soup.find('div', {'class':'LocationList tw'})
    else:
        elem = soup.find('div', {'class':'LocationList oversea'})
    
    
    # list
    venue_list=[]
    for venue in elem.find_all('div', {'class':'item'}):
        
        # entry name
        name = venue.find('div', {'class':'title'}).text
        logging.info('processing:{}'.format(name))
        data = {'region': region_mapping[str(region)], 'name': name}
        # when name contain '/' meaning both organization shared same venue
        # we can dup the contact information, but use unique name
        # contaxts:
        contacts = venue.find('ul',{'class':'contact_info'})
        contacts_lines = contacts.text.strip().splitlines()
        # example of this string...
        for line in contacts_lines:
            fields = line.split('：')
            if region == 1:
                if fields[0] in field_mapping:
                    data[field_mapping[fields[0]]]=' '.join(fields[1:])
            else:
                data[fields[0]] = ' '.join(fields[1:])

        # url/traffic/map section
        btns = contacts.find_next_sibling('div',{'class':'btns'})
        url = btns.find('a', {'class':'url'})
        if url:
            data['url'] = url['href']

        # checking any critical field missing...
        if region == 1:        
            if '/' in name:
                for n in name.split('/'):
                    data['name']=n.strip()
                    temp_data=data.copy()
                    venue_list.append(temp_data)
            else:
                venue_list.append(data)  
        else:
            venue_list.append(data)

    return venue_list


# section on pytdantic model and validation
from pydantic import BaseModel, Field, HttpUrl, EmailStr, model_validator
from typing import ClassVar, Optional, Self
import re

class Venue(BaseModel):

    name: str
    region: str
    Tel: Optional[str] = Field(default=None)
    Address: Optional[str] = Field(default=None)
    Mail: Optional[EmailStr] = Field(default=None)
    url: Optional[HttpUrl] = Field(default=None)
    Fax: Optional[str] = Field(default=None)
    Contact: Optional[str] = Field(default=None)

    # these parameter are provided after 'validation/parsing'
    Zipcode: Optional[str]=Field(default=None, min_length=3, max_length=6)
    FullAddress:  Optional[str]=Field(default=None, min_length=1, max_length=255)

    # ClassVar won't be validated..
    regex_location: ClassVar = r'(\d{5,6})?\s*([\S\s]*)'

    # validation on Address
    @model_validator(mode='after')
    def validate_address(self) -> Self:
        # parse the full address
        if self.region == 'tw' and self.Address is not None:
            logging.info(self.Address)
            matches = re.match(self.regex_location, self.Address)
            postal_code, full_address = matches.groups()
            logging.info('{}:{}'.format(postal_code, full_address))
            self.Zipcode=postal_code
            self.FullAddress=full_address
        return self

if __name__ == '__main__':
    
    logging.basicConfig(level=logging.INFO)

    # download all venues from website
    venues = download_venues(region=1)
    venues.extend(download_venues(region=2))

    # loop, element is dict, try to json parse it
    # we should have all unique name, but address would be one to one
    try:
    
        for venue in venues:
            
            venue_valitated = Venue.model_validate(venue)
            
            logging.info(venue_valitated)

            #logging.info(json.dumps(venue, ensure_ascii=False, indent=4))
            #response = requests.post('http://127.0.0.1:8000/venues/', json = venue_valated.model_dump(mode='json'))
            #if response.status_code != 200:
            #    raise RuntimeError (response.content)
            
            #logging.info(response.json())
            
    except RuntimeError as e:
        logging.error(e)