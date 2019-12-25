import random
from sched import scheduler
from time import time, sleep

import requests
import sys
from loguru import logger

from parse_excel import make_migrate, lead_checkout, get_last_stop_pk, \
    get_lead, get_cnt_lead_sent, reset_lead_sent

logger.add(sys.stderr, format="{time} {level} {message}",
           filter="my_module", level="DEBUG")
logger.add("/tmp/brosproj.log", rotation="10 MB")
s = scheduler(time, sleep)
random.seed()


def send_to_lavie(pk, full_name, email, phone_number):
    jsnn = {"hit": {"page_id": 2317103, "ab_id": 3223814, "referer": "",
                    "uri": "/"}, "form": {"name": "Know more", "type": "order",
                                          "integrations": ["20442", "41905",
                                                           "60631", "73708"],
                                          "after": "msg",
                                          "msg": "Thank you! Our property consultant will contact you soon.",
                                          "url": "http://mpd.ae/wp-content/uploads/Meraas-Brochure_Port-De-La-Mer.pdf",
                                          "addhtml": "",
                                          "js": "alert(\"Этот код выполняется после успешного отправления заявки.\");"},
            "item": [], "items": [], "fields": [
            {"name": "Full name", "type": "name", "required": True, "id": "",
             "value": full_name},
            {"name": "Phone number", "type": "phone", "required": True,
             "id": "", "value": phone_number},
            {"name": "E-mail", "type": "email", "required": True, "id": "",
             "value": email},
            {"name": "How many bedrooms?", "type": "select-menu",
             "required": True, "id": "bedroom-count", "value": "1-bedroom"},
            {"name": "country", "type": "hidden", "required": True,
             "id": "hiddenname",
             "value": "UAE / undefined / https://iplogger.ru/ip-lookup/?d=null"},
            {"name": "", "type": "hidden", "required": True, "id": "entity",
             "value": "MPP"},
            {"name": "", "type": "hidden", "required": True, "id": "country",
             "value": "UAE"},
            {"name": "", "type": "hidden", "required": False, "id": "developer",
             "value": "Dubai Properties"},
            {"name": "", "type": "hidden", "required": True, "id": "project",
             "value": "La Vie JBR"},
            {"name": "", "type": "hidden", "required": True, "id": "bx24",
             "value": "1"},
            {"name": "", "type": "hidden", "required": True, "id": "language",
             "value": "en"}]}

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36',
        'Host': 'la-vie-jumeirah-beach-residences.ae',
        'Origin': 'https://la-vie-jumeirah-beach-residences.ae',
        'Referer': 'https://la-vie-jumeirah-beach-residences.ae/',
        'X-Requested-With': 'XMLHttpRequest',
        'Content-Type': 'application/json'}

    r = requests.post('https://la-vie-jumeirah-beach-residences.ae/app/c',
                      headers=headers, verify=True,
                      json=jsnn)

    if r.status_code == 200:
        lead_checkout(pk, sent_address=True)
        logger.debug(r.content)


def send_to_one_jbr(pk, full_name, email, phone_number):
    jsnn = {"hit": {"page_id": 1793085, "ab_id": 2408685, "referer": "",
                    "uri": "/"},
            "form": {"name": "1/JBR / Register your interest", "type": "order",
                     "integrations": ["20442", "41905", "73708", "75141"],
                     "after": "msg+addhtml",
                     "msg": "Thank you! Our property consultant will contact you soon.",
                     "url": "/",
                     "addhtml": "<script>\nfbq('track', 'Lead');\n</script>",
                     "js": "alert(\"Этот код выполняется после успешного отправления заявки.\");"},
            "item": [], "items": [], "fields": [
            {"name": "Full name", "type": "name", "required": True, "id": "",
             "value": full_name},
            {"name": "Phone number", "type": "phone", "required": True,
             "id": "", "value": phone_number},
            {"name": "E-mail", "type": "email", "required": True, "id": "",
             "value": email},
            {"name": "How many bedrooms", "type": "select-menu",
             "required": True, "id": "bedroom-count", "value": "2-bedroom"},
            {"name": "country", "type": "hidden", "required": True,
             "id": "hiddenname",
             "value": "UAE / undefined / https://iplogger.ru/ip-lookup/?d=null"},
            {"name": "", "type": "hidden", "required": True, "id": "entity",
             "value": "MPP"},
            {"name": "", "type": "hidden", "required": True, "id": "country",
             "value": "UAE"},
            {"name": "", "type": "hidden", "required": False, "id": "developer",
             "value": "Dubai Properties"},
            {"name": "", "type": "hidden", "required": True, "id": "bx24",
             "value": "1"},
            {"name": "", "type": "hidden", "required": True, "id": "project",
             "value": "One Jbr"},
            {"name": "", "type": "hidden", "required": True, "id": "language",
             "value": "en"}]}

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36',
        'Host': 'one-jbr.ae',
        'Origin': 'https://one-jbr.ae',
        'Referer': 'https://one-jbr.ae/',
        'X-Requested-With': 'XMLHttpRequest',
        'Content-Type': 'application/json'}

    r = requests.post('https://one-jbr.ae/app/c', verify=True,
                      headers=headers,
                      json=jsnn)

    if r.status_code == 200:
        lead_checkout(pk, sent_address=True)
        logger.debug(r.content)


def send_to_the_address(pk, full_name, email, phone_number):
    jsnn = {"hit": {"page_id": 2280968, "ab_id": 3161471, "referer": "",
                    "uri": "/"}, "form": {"name": "Know more", "type": "order",
                                          "integrations": ["20442", "41905",
                                                           "60631", "73708"],
                                          "after": "msg",
                                          "msg": "Thank you! Our property consultant will contact you soon.",
                                          "url": "http://mpd.ae/wp-content/uploads/Meraas-Brochure_Port-De-La-Mer.pdf",
                                          "addhtml": "",
                                          "js": "alert(\"Этот код выполняется после успешного отправления заявки.\");"},
            "item": [], "items": [], "fields": [
            {"name": "Full name", "type": "name", "required": True, "id": "",
             "value": full_name},
            {"name": "Phone number", "type": "phone", "required": True,
             "id": "", "value": phone_number},
            {"name": "E-mail", "type": "email", "required": True, "id": "",
             "value": email},
            {"name": "country", "type": "hidden", "required": True,
             "id": "hiddenname",
             "value": "UAE / undefined / https://iplogger.ru/ip-lookup/?d=null"},
            {"name": "", "type": "hidden", "required": True, "id": "entity",
             "value": "MPP"},
            {"name": "", "type": "hidden", "required": True, "id": "country",
             "value": "UAE"},
            {"name": "", "type": "hidden", "required": False, "id": "developer",
             "value": "Al Ain Properties"},
            {"name": "", "type": "hidden", "required": True, "id": "project",
             "value": "Address Jumeirah Resort + SPA"},
            {"name": "", "type": "hidden", "required": True, "id": "bx24",
             "value": "1"},
            {"name": "", "type": "hidden", "required": True, "id": "language",
             "value": "en"}]}

    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36',
               'Host': 'the-address-residences-jumeirah.ae',
               'Origin': 'https://the-address-residences-jumeirah.ae',
               'Referer': 'https://the-address-residences-jumeirah.ae/',
               'X-Requested-With': 'XMLHttpRequest',
               'Content-Type': 'application/json'}
    r = requests.post('https://the-address-residences-jumeirah.ae/app/c',
                      headers=headers, verify=True,
                      json=jsnn)

    if r.status_code == 200:
        lead_checkout(pk, sent_address=True)
        logger.debug(r.content)


def cron_job(start, end, interval, func):
    event_time = start
    while event_time < end:
        s.enterabs(event_time, 0, func, ())
        rtime = random.randint(-300, 900)
        event_time += interval + rtime
        logger.debug(rtime)
    s.run()


def get_data():
    last_id = get_last_stop_pk()
    logger.debug('Last ID {}'.format(last_id))
    leads_to_send = get_lead(last_id)
    sent_limit = random.randint(1, 10)
    num_sent = 0
    for lds in leads_to_send:
        logger.debug(lds)
        cnt = get_cnt_lead_sent('sent_address')
        if cnt <= sent_limit:
            send_to_the_address(lds[0], lds[1], lds[2],
                                lds[3] if lds[3] else lds[4])
            num_sent = 1
            continue
        cnt = get_cnt_lead_sent('sent_onejbr')
        if cnt <= sent_limit:
            send_to_one_jbr(lds[0], lds[1], lds[2],
                            lds[3] if lds[3] else lds[4])
            num_sent = 1
            continue
        cnt = get_cnt_lead_sent('sent_lavie')
        if cnt <= sent_limit:
            send_to_lavie(lds[0], lds[1], lds[2],
                          lds[3] if lds[3] else lds[4])
            num_sent = 1
            continue

        if num_sent == 0:
            reset_lead_sent()

    logger.debug(leads_to_send)


if __name__ == '__main__':
    #make_migrate()
    cron_job(time() + 5, time() + 604800, 1800, get_data)
    #read_xls('junks_for_sardor.xlsx')
    #app.run()
