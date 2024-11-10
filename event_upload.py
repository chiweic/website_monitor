# this is deal with all HTML download from crawler, and to upload to db over the cloud
# what we care is the detail html pages
import logging
from bs4 import BeautifulSoup
import glob
import re
import time as system_time
# from dotenv import load_dotenv
# from supabase import create_client, Client
from urllib.parse import urljoin, quote
import requests

#load_dotenv()
#url: str = os.environ.get("SUPABASE_URL")
#key: str = os.environ.get("SUPABASE_KEY")
#supabase: Client = create_client(url, key)



def upload_schedule(schdule):
    pass
    

def parse_address(address_string):
    # Regular expression pattern
    pattern = r"(.*?)\s*\((.*?)\)"
    
    # Try to match the pattern
    match = re.match(pattern, address_string)
    
    if match:
        name = match.group(1).strip()
        address = match.group(2).strip()
        return name, address
    else:
        return None, None


def filter_text(input_text):
    return input_text.strip().replace('\n','').replace('\r','')


def fetch_with_retry(url, max_retries=3, retry_delay=5, condition_check=lambda x: True):
    retries = 0
    while retries < max_retries:
        try:
            response = requests.get(url)
            if response.status_code == 200 and condition_check(response.content):    
                return response.content
            else:
                logging.error(f"Attempt {retries + 1} failed: Condition not met or unexpected response code ({response.status_code}). Retrying...")
        except requests.RequestException as e:
            logging.error(f"Attempt {retries + 1} failed: {e}. Retrying...")
        
        retries += 1
        system_time.sleep(retry_delay)
        
    raise Exception("Max retries reached. Failed to fetch valid content.")

def content_check (content):
    required_tags = ["活動單位", "活動地點"]
    content_str = content.decode('utf-8', errors='ignore')
    if len(content_str) > 128:
        return True
    else:
        return False
    #return all(tag in content_str for tag in required_tags)


# schedule/location test cases: A schedule "might" have multiple locations
# will move this part of code to a util areas,
# we go back and forth on adopting 'cheinese name' or a translted to name the dict keys
# will most likely go with Eng as it avoid all the funny utf (seen from cmd as \u...)
def parsing_schedule (schedule_url):
    
    # debugging information
    logging.info('parsing_schedules {}'.format(schedule_url))
    
    # all functions will raise RuntimeError if bad thing happened
    try:
                
            # read, this get irritated when resource become issue
            # we need to do a checking on reading
            content = fetch_with_retry(url=schedule_url, max_retries=5, retry_delay=3,
                                       condition_check=content_check)

            # bs4 based parsing
            soup = BeautifulSoup(content, 'html.parser')
            
            # a new schedule
            schedule={}

            # write the timestamp on when we did the crawling
            schedule['download_at'] = datetime.now()

            # write mapping 活動鏈接
            schedule['url'] = schedule_url

            # tags/category, it should comply with <a class='cat_main> <
            tags = soup.find('div',{'class':'tags_btn'})
            # cat_main, cat_sub, and type: 分類
            schedule['tags']={}
            schedule['tags']['main']=tags.find('a', {'class':'cat_main'}).text
            schedule['tags']['sub']=tags.find('a', {'class':'cat_sub'}).text
            schedule['tags']['type']=tags.find('a', {'class':'type'}).text

            # title 活動名稱
            title = soup.find('h3',{'class':'event_cont_title'})
            if title == None:
                raise RuntimeError('error parsing title tag')
            schedule['title']=title.text

            # venue finder 活動單位
            venue = soup.find('th',string='活動單位').find_next_sibling('td',{'class':'cont'})
            if venue == None:
                raise RuntimeError('error parsing venue tag')
            schedule['organizer']=venue.text

            # 活動網址 (optional)
            venue_url = soup.find('th',string='活動網址')
            if venue_url:
                venue_url=venue_url.find_next_sibling('td',{'class':'cont'})
                schedule['organizer_url']=venue_url.text
            
            # datetime is a MUST...
            datetime_url = soup.find('th',string='活動日期及時間')
            if datetime_url == None:
                raise RuntimeError('missing datetime')
            
            datetime_url=datetime_url.find_next_sibling('td',{'class':'cont'})
            schedule['date_time'] = datetime_url.text

            
            # location tag finder 活動地點
            location = soup.find('th',string='活動地點')
            if location == None:
                raise RuntimeError('error parsing location tag')

            # right box contain only text
            location_box= location.find_next_sibling('td', {'class':'cont'})
            locations= location_box.find_all('div', {'class':'text'})
            
            schedule['locations']='&& '.join([loc.text for loc in locations])
            

            # description (optional) 活動描述
            description = soup.find('div', {'class':'event_text_box'})
            if description:
                text_filter = filter_text(description.text)
                if text_filter:
                    schedule['descriptions']=text_filter


            # registration details
            registration = soup.find('h4', class_='sub_title tt-signup', string='活動報名')
            if registration:
                # audience and registration period
                sections = registration.find_next_sibling('ul',{'class':'event_list'})
                if sections:
                    
                    audience = sections.find('div', class_='tt', string='報名對象')
                    if audience:
                        schedule['target_audience']=(audience.find_next_sibling('div', {'class':'text'})).text
                        #logging.info((audience.find_next_sibling('div', {'class':'text'})).text)
                    registration_period = sections.find('div', class_='tt', string='報名時間')
                    if registration_period:
                        text_filter = filter_text(registration_period.find_next_sibling('div', {'class':'text'}).text)                            
                        schedule['registraion_period']=text_filter
                            
                else:
                    raise RuntimeError('error parsing registration')
                                
            # events parsing
            events_section = soup.find('h4', class_='sub_title tt-schedule', string='活動場次')
            if events_section == None:
                raise RuntimeError ('missing events schedules')
            event_schedule = events_section.find_next_sibling('div', {'class':'event_schedule'})
            # enumerate schedule*: multiple possible
            schedule['sessions']=[]
            
            for item in event_schedule.find_all('div',{'class':'item'}):
                    
                # new dict containing items
                event_item={}
                # top title and signups
                top_title =item.find('div', {'class':'top_title'})
                # the top title contained formated text
                session_order = top_title.find('div', {'class':'light'})
                session_title = top_title.find('div', {'class':'tt'})
                    
                event_item['session'] = session_order.text
                event_item['theme'] = session_title.text
                
                top_signup=item.find('div', {'class':'top_signup'})
                status = top_signup.find('span',{'class':'status'})
                # optional status
                if status:
                    logging.debug(status.text)
                    event_item['event_status']=status.text


                event_item['events']=[]
                # list all the events
                event_list = item.find('div', {'class':'ListTable time_list'})
                for event in event_list.find_all('tr')[1:]:
                        
                    # date and time
                    event_date = event.find('td', {'class':'date'})
                    event_time = event.find('td', {'class':'time'})

                    # this event location must be expressed from schedule already
                    event_location = event_time.find_next_sibling('td')
                    logging.debug(event_location)

                    # add to the list
                    event_item['events'].append(
                            {
                             'date':event_date.text,
                             'time': event_time.text,
                             'location':event_location.text
                            }
                        )
                           

                schedule['sessions'].append(event_item)

                    

    except RuntimeError as e:
        
        logging.error('error parsing this html: {}'.format(e))
        schedule=None
    
    except Exception as e:

        logging.error('other error: {}'.format(e))
        schedule=None
    
    finally:
        
        return schedule


# compare two json (by stripping its pk)
def compare_schedules(src_json, db_json):

    logging.debug('source: {}'.format(sorted(src_json)))
    logging.debug('target: {}'.format(sorted(db_json)))
    # care about: description, locations, schedule_datetime, title, url, venue and venue_url
    # for field in ['description', 'locations', 'schedule_datetime','title', 'venue', 'venue_url']:
    #    if field in src_json and field in db_json:
    #        if src_json[field] != db_json[field]:
    #            return False
    
    return True


from pydantic import AnyUrl, BaseModel, Field, ConfigDict, HttpUrl, field_serializer, field_validator
from datetime import datetime, time, date
from typing import ClassVar, Dict, List, Optional
import uuid
import re

class EventBase(BaseModel):
    model_config = ConfigDict(extra='forbid')
    
    start_time: time
    end_time: time
    event_date: date
    location: str

class Event(EventBase):
    pk: uuid.UUID


class EventCreate(EventBase):    
    section_pk: uuid.UUID

    
class EventPublic(EventBase):
    pass

class SectionBase(BaseModel):
    model_config = ConfigDict(extra='forbid')

    sequence: str = Field(min_length=1, max_length=63)
    title:str = Field(min_length=1, max_length=155)
    status: Optional[str]=Field(default=None, min_length=1, max_length=63)


class Section(SectionBase):
    pk: uuid.UUID
    events: List['Event']


class SectionCreate(SectionBase):
    schedule_pk: uuid.UUID

    #@field_serializer('schedule_pk')
    #def serialize_url(self, pk, _info):
    #    return str(pk)

class SectionPublic(SectionBase):
    events: List['EventPublic']

# regex_dt is NOT a field, so specify as ClassVar
class ScheduleBase(BaseModel):
    model_config = ConfigDict(extra='forbid')
    # private member used to parse/validate datetime
    

    # fields that need to be included
    url: HttpUrl 
    title: str = Field(min_length=1, max_length=155)
    venue: str = Field(min_length=1, max_length=127)
    venue_url: Optional[AnyUrl]=Field(default=None)
    description: Optional[str]=Field(default=None, min_length=1, max_length=1023)
    locations: Dict[str, str] = Field(default=None)
    registration: Optional[Dict[str, str]]=Field(default=None)
    schedule_datetime: str = Field(min_length=1, max_length=155)


class Schedule(ScheduleBase):
    pk: uuid.UUID    
    sections: List['Section']


class ScheduleCreate(ScheduleBase):

    regex_location: ClassVar = r"(\d{3,5})?(.*自宅)?(.*)$"
    regex_dt: ClassVar = r"(\d{4}/\d{2}/\d{2})～(\d{4}/\d{2}/\d{2})，(\d{2}:\d{2})～(\d{2}:\d{2})"

    # validate location dict, set check_fields if optional
    @field_validator ('locations')
    @classmethod
    def location_format_compliance(cls, v: Dict) -> Dict:
        # values must confirmed to the regex
        for value in v.values():
            matches = re.match(cls.regex_location, value)
            if not matches:
                raise ValueError('problematic format on location {}'.format(value))
            # if one need to find the postcode and full address
            zipcode, fulladdress, home = matches.groups()
        return v

    # validate datetime 
    @field_validator ('schedule_datetime')
    @classmethod
    def datetime_format_compliance(cls, d: str) -> str:
        matches = re.match(cls.regex_dt, d)
        if not matches:
            raise ValueError('problematic format on datetime {}'.format(d))
        start_date, end_date, start_time, end_time = matches.groups()
        
        return d

    @field_validator('venue_url', mode='before')
    @classmethod
    def validate_venue_url (cls, venue_url: AnyUrl) -> AnyUrl:
        if venue_url.startswith('www') or venue_url.startswith('ft.ddm'):
            return 'https://'+venue_url
        return venue_url


# we like to inherit ScheduleCreate to make use of all validators
class SchedulePublic(ScheduleCreate):
    sections: List['SectionPublic']


# helper to post to server that only do CRUD on each level
# the following question describe difference from using data or json in the requests parameter
# using data, meerely meaning headers would need to be set manually, json will auto set as json
# but we are sending over plain string anyway, so things will need to be "serialized"
# https://stackoverflow.com/questions/26685248/difference-between-data-and-json-parameters-in-python-requests-package
# https://stackoverflow.com/questions/72801333/how-to-pass-url-as-a-path-parameter-to-a-fastapi-route
# sending URL over will need to be 'quoted
from urllib.parse import quote
def submit_schedule(schedule):
    
    # look up schedule with the url
    logging.info(schedule['url'])
    response = requests.get('http://127.0.0.1:8000/schedule_by_url/'+quote(schedule['url'], safe=''))
    
    if response.status_code == 200:
        logging.debug('existing enrty')    
        # pk = response.json()['pk']
        # response = requests.delete('http://127.0.0.1:8000/schedules/'+pk)
        return {'reason':'existing record'}
    
    # thing other than 200 means not found...
    # validate the "schedule" items on the creation class 
    sections=schedule['sections'].copy()
    del schedule['sections']
    # exceptions: venue_url start with www instead of http..
    if 'venue_url' in schedule and schedule['venue_url'].startswith('www'):
        schedule['venue_url']='https://'+schedule['venue_url']
    # validate schedule
    schedule_validated=ScheduleCreate.model_validate(schedule)
    response = requests.post('http://127.0.0.1:8000/schedules/', json=schedule_validated.model_dump(mode='json'))
    if response.status_code != 200:
        raise RuntimeError(response.text)
    schedule_pk = response.json()['pk']
    for section in sections:
        events=section['events']
        del section['events']
        # validate section
        section['schedule_pk']=schedule_pk
        section_validated = SectionCreate.model_validate(section)
        response = requests.post('http://127.0.0.1:8000/sections/', json=section_validated.model_dump(mode='json'))
        if response.status_code != 200:
            raise RuntimeError(response.text)
        section_pk = response.json()['pk']
        for event in events:
            event['section_pk']=section_pk
            event_created = EventCreate.model_validate(event)
            response = requests.post('http://127.0.0.1:8000/events/', json=event_created.model_dump(mode='json'))
            if response.status_code != 200:
                raise RuntimeError(response.text)
            
    return {'creation':'ok'}

# the followings are example of client side requests
def example_requests():
    
    # get list
    response = requests.get('http://127.0.0.1:8000/schedules/', params={'offset':0, 'limit':100})
    if response.status_code != 200:
            raise RuntimeError(response.text)
    logging.info(len(response.json()))
    # example of get a schedule by pk
    pk = 'b37b6bbc-ebcd-494d-a61e-4cf79fcbda66'
    response = requests.get('http://127.0.0.1:8000/schedules/'+ pk)
    if response.status_code != 200:
            raise RuntimeError(response.text)
    logging.info(json.dumps(response.json(), indent=4, ensure_ascii=False))
    # modification on an schdule (but only on main body)
    payload = response.json()
    del payload['pk']
    del payload['sections']

    payload['venue'] = '基隆精舍'
    response = requests.patch('http://127.0.0.1:8000/schedules/'+ pk, json=payload)
    if response.status_code != 200:
            raise RuntimeError(response.text)
    logging.info(json.dumps(response.json(), indent=4, ensure_ascii=False))
    exit(0)


def sanity_check():

    # retrieve number of schedules
    response = requests.get('http://127.0.0.1:8000/schedules_pks/')
    if response.status_code != 200:
            raise RuntimeError(response.text)    
    pks = response.json()
    # retrieve with the pks
    for pk in pks:
        response = requests.get('http://127.0.0.1:8000/schedules/'+pk)
        if response.status_code != 200:
            raise RuntimeError(response.text)    
        # sanity check
        # null section?
        schedule = response.json()
        if 'sections' not in schedule:
            logging.error(schedule)
        if len(schedule['sections'])<1:
            logging.error(schedule)
            
        # for each section
        for section in schedule['sections']:
            if 'events' not in section or len(section['events'])<1:
                logging.error(schedule)
        

# upload all schedules to webserver
# save_intermediate will save the schedule in the raw form to json
def download_schedules(calendar_year='2024', save_intermediate='raw_output.json'):

    html_links=[]
    # parsing crawled html from crawler
    # base date is /mnt/vol-2/repos/ddm_data/html_output/2024-10-02
    for fname in glob.glob('/mnt/vol-2/repos/ddm_data/html_output/{}*/*.html'.format(calendar_year)):
        logging.info('process {}'.format(fname))
        with open (fname, 'r') as f:
            soup = BeautifulSoup(f.read(), "html.parser")
            root_url = soup.find('a', {'title':'法鼓山全球資訊網'})['href']
            for elem in soup.find_all('div',{'class':'col_right'}):
                link = elem.find('span', {'class':'more_btn'}).find('a', href=True)['href']
                html_links.append(urljoin(root_url,link))
    
    # simple stats
    logging.info('Total {}=>{} links extracted'.format(len(html_links), len(set(html_links))))
    
    # aging link with no session and datetime
    # html_links = ['https://www.ddm.org.tw/xcevent/cont?xsmsid=0K293423255300198901&en=A202400046']
    # html_links=['https://www.ddm.org.tw/xcevent/cont?xsmsid=0K293423255300198901&en=A202100730']
    # html_links=['https://www.ddm.org.tw/xcevent/cont?xsmsid=0K293423255300198901&en=A202401796']
    # html_links = ['https://www.ddm.org.tw/xcevent/cont?xsmsid=0K293423255300198901&en=A201900903']
    # html_links= ['https://www.ddm.org.tw/xcevent/cont?xsmsid=0K293423255300198901&en=A202200015']
    # main loop
    try:

        # save to the raw loading
        fp = open(save_intermediate, 'w', encoding='utf-8') 
        
        for link in sorted(set(html_links), reverse=True):
                
            # try to parse this, load this as object
            schedule = parsing_schedule(schedule_url=link)
            # parsing schedule result in a dict with strings
            logging.debug(schedule)

            # write to output file if not null
            if schedule:
                json.dump(schedule, fp=fp, ensure_ascii=False, default=str)
                fp.write('\n')
                fp.flush()


    except RuntimeError as err:
        
        logging.error(err)
        
    finally:
        fp.close()


def upload_schedules(rawoutput_json):
    
    data=[]
    with open (rawoutput_json,'r') as fp:
       for line in fp:
            data.append(json.loads(line))
    
    for d in data:
        
        logging.info('process {}'.format(d['url']))
        response = requests.get('http://127.0.0.1:8000/schedule_by_url/'+quote(d['url'], safe=''))
        if response.status_code == 200:
            logging.info('existing enrty')    

        else:
            # no entry from database, ready to do post
            v = SchedulePublic.model_validate(d)
            schedule=v.model_dump(mode='json', exclude={'sections'})
            # post this info to server, and obtain pk from db
            response = requests.post('http://127.0.0.1:8000/schedules/', json=schedule)
            if response.status_code != 200:
                raise RuntimeError(response.text)
            schedule_pk = response.json()['pk']

            # now we should have a validated json on top level ...
            for s in v.sections:
                section = s.model_dump(mode='json', exclude={'events'})
                section.update(schedule_pk=schedule_pk)
                logging.debug(section)
                response = requests.post('http://127.0.0.1:8000/sections/', json=section)
                if response.status_code != 200:
                    raise RuntimeError(response.text)
                section_pk = response.json()['pk']
                for e in s.events:
                    event = e.model_dump(mode='json')
                    event.update(section_pk=section_pk)
                    response = requests.post('http://127.0.0.1:8000/events/', json=event)
                    if response.status_code != 200:
                        raise RuntimeError(response.text)



from random import shuffle
# doing a model_dump from webserver
# these are all validated and pull out from production databases
def dump_schedules(output_file, random_selections=False, top_N=100):
    
    # dump everything but exclude pks
    # retrieve number of schedules
    response = requests.get('http://127.0.0.1:8000/schedules_pks/')
    if response.status_code != 200:
            raise RuntimeError(response.text)    
    pks = response.json()
    # if randomly slection, than choose topN
    if random_selections:
        pks = list(shuffle(pks))[:top_N]                                                   
    

    # retrieve with the pks
    schedules=[]
    
    # select schedule to download
    for pk in pks:
        response = requests.get('http://127.0.0.1:8000/schedules/'+pk)
        if response.status_code != 200:
            raise RuntimeError(response.text)    
        schedules.append(response.json())

    with open(output_file, 'w') as fp:
        json.dump(schedules, fp, indent=4, ensure_ascii=False)





import json
if __name__ == '__main__':
    
    logging.basicConfig(level=logging.INFO)
    
    # one should try to download all schedule as close as possible
    download_schedules(calendar_year='2021', save_intermediate='2021_rawoutput.json')
    download_schedules(calendar_year='2022', save_intermediate='2022_rawoutput.json')
    download_schedules(calendar_year='2023', save_intermediate='2023_rawoutput.json')
    download_schedules(calendar_year='2024', save_intermediate='2024_rawoutput.json')
    download_schedules(calendar_year='2025', save_intermediate='2025_rawoutput.json')
    
    #upload_schedules(rawoutput_json='2025_rawoutput.json')
    # dump all event in the future, commbining all relevant info
    # use section_pk and schedule_pk to locate all top level info
    
    exit(0)

    #pk = 'b37b6bbc-ebcd-494d-a61e-4cf79fcbda66'
    response = requests.get('http://127.0.0.1:8000/events_pks/')
    if response.status_code != 200:
            raise RuntimeError(response.text)
    
    with open('demo.jsonl', 'w') as fp:
        
        for pk in response.json():

            # get event with this pk
            response = requests.get('http://127.0.0.1:8000/events/'+pk)
            if response.status_code != 200:
                raise RuntimeError(response.text)
            event = response.json()
            response = requests.get('http://127.0.0.1:8000/sections/'+event['section_pk'])
            if response.status_code != 200:
                raise RuntimeError(response.text)
            section=response.json()
            logging.debug(section)
            response = requests.get('http://127.0.0.1:8000/schedules/'+section['schedule_pk'])
            if response.status_code != 200:    
                raise RuntimeError(response.text)
            schedule =response.json()
            logging.debug(schedule)
            # fields relevants and create a new model
            # schedule.title, schedule.venue, schedule.location, registration, description.
            # section.status
            # event start-end time and event_date
            fields={'活動名稱': schedule['title'],
                    '活動描述': schedule['description'],
                    '活動地點與地址': schedule['locations'],
                    '活動日期': event['event_date'],
                    '開始時間':event['start_time'],
                    '結束時間': event['end_time']
                    }
            
            if section['status']:
                fields.update(報名狀況=section['status'])
            
            logging.info(fields)
            json.dump(fields, fp, ensure_ascii=False)
            fp.write('\n')