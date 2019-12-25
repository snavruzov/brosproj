import pandas as pd
from sqlalchemy import create_engine, Column, Integer, Boolean, insert
from sqlalchemy.ext.declarative import declarative_base

engine = create_engine('postgresql://test:test@localhost:5432/smart_admin')
Base = declarative_base()


def read_xls(file_name):
    dfs = pd.read_excel(file_name)
    df = pd.DataFrame(dfs)
    df.to_sql('leads', engine, chunksize=100, index=False)


class DepartmentEmployee(Base):
    __tablename__ = 'lead_settings'
    id = Column(Integer, primary_key=True)
    lead_id = Column(Integer)
    sent_address = Column(Boolean)
    sent_onejbr = Column(Boolean)
    sent_lavie = Column(Boolean)
    sent_laviejbr = Column(Boolean)
    sent_opr = Column(Boolean)


def make_migrate():
    Base.metadata.create_all(engine)


def get_last_stop_pk():
    qr = ('SELECT "lead_id" FROM lead_settings '
          'ORDER BY "id" DESC LIMIT 1')
    with engine.connect() as con:
        rs = con.execute(qr)
        last_id = rs.fetchone()

    return last_id[0] if last_id else 0


def get_cnt_lead_sent(sent_web):
    qr = ('SELECT COUNT("id") FROM lead_settings '
          'WHERE {}=TRUE').format(sent_web)
    with engine.connect() as con:
        rs = con.execute(qr)
        cnt = rs.fetchone()

    return cnt[0]


def reset_lead_sent():
    qr = 'UPDATE lead_settings SET ' \
         'sent_address = FALSE, ' \
         'sent_onejbr = FALSE, ' \
         'sent_lavie = FALSE, ' \
         'sent_laviejbr = FALSE, ' \
         'sent_opr = FALSE;'
    with engine.connect() as con:
        con.execute(qr)


def lead_checkout(lead_id,
                  sent_address=False,
                  sent_onejbr=False,
                  sent_lavie=False,
                  sent_laviejbr=False,
                  sent_opr=False):
    # leads_sent = DepartmentEmployee(lead_id=lead_id,
    #                                 sent_onejbr=sent_onejbr,
    #                                 sent_lavie=sent_lavie,
    #                                 sent_laviejbr=sent_laviejbr,
    #                                 sent_address=sent_address,
    #                                 sent_opr=sent_opr)
    with engine.connect() as con:
        con.execute("INSERT INTO lead_settings (lead_id, sent_onejbr, "
                    "sent_lavie, sent_laviejbr, sent_address, sent_opr) "
                    "VALUES ({}, {}, {}, {}, {}, {})".format(
                        lead_id, sent_onejbr, sent_lavie,
                        sent_laviejbr, sent_address, sent_opr))


def get_lead(last_id=0):
    qr = ('SELECT "ID", "Lead Name", '
          '"Home E-mail", "Work Phone", "Mobile" '
          'FROM leads '
          'LEFT JOIN lead_settings ON lead_settings.lead_id <> leads."ID" '
          'WHERE "Home E-mail" IS NOT NULL '
          'AND ("Mobile" IS NOT NULL OR "Work Phone" IS NOT NULL) '
          'AND "Home E-mail" is NOT NULL  and "Lead Name" <> \'LEAD\''
          'AND leads."ID" > {} ORDER BY leads."ID" ASC LIMIT 1').format(last_id)
    lead_list = []
    with engine.connect() as con:

        rs = con.execute(qr)
        for row in rs:
            print(row)
            lead_list = [[row[0], row[1], row[2], row[3], row[4]]]

    return lead_list

