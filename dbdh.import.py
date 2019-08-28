import sqlite3
import re
from pathlib import Path
from urllib.parse import urlparse

#For configuration. Do we want it?
import yaml

from tidylib import tidy_document

#SFTP support (for importing old data from Worf):
#TODO: get it working.
import paramiko

#WORF = paramiko.SSHClient()
#print(dict(WORF.get_host_keys()))
#WORF.load_host_keys(r'c:\\users\\ab24\\.ssh\\known_hosts')
#WORF.set_missing_host_key_policy(paramiko.AutoAddPolicy())

from lxml import etree as ET


COLLECTION = ET.parse('bdh_collection.xml')

#TODO: Get keys working here.



ns = {'mets': 'http://www.loc.gov/METS/',
      'mods': 'http://www.loc.gov/mods/v3',
      'mix': 'http://www.loc.gov/mix/',
      'xlink': 'http://www.w3.org/1999/xlink'}


#Move to YAML?
NEW_ISSUE = 'INSERT INTO issues(date, mets, pdf, label) VALUES (?, ?, ?, ?)'
NEW_PAGE = 'INSERT INTO pages(issue_id, page_num, jp2, jpg) VALUES (?, ?, ?, ?)'

db = sqlite3.connect('./dbdh.db')
db.row_factory = sqlite3.Row
c = db.cursor()

def _getDate(xml, filename):
    date = xml.find('.//mods:dateIssued', ns)

    (dy, mth, yr) = (False, False, False)

    date = date.text.strip()

    if len(date) not in (10, 7):
        #Not a whole date. Need to get from a file path.
        filename = xml.find('.//mix:SourceData', ns).text
        find = re.compile(r'\/(?P<year>[\d]{4})/(?P<month>[\d]{2})/(?P<day>[\d]{2})\_')
        m = re.search(find, filename)
        if m:
            dy = m.group('day')
            mth = m.group('month')
            yr = m.group('year')
        else:
            find = re.compile(r'\/(?P<year>[\d]{4})/(?P<month>[\d]{2})/')
            m = re.search(find, filename)
            if m:
                dy = None
                mth = m.group('month')
                yr = m.group('year')
    
    if not yr:
        try:
            (dy, mth, yr) = date.split('.')
        except:
            pass
    
    if not yr:
        try:
            (yr, mth, dy) = date.split('-')
        except:
            pass
    
    if not yr:
        try:
            (dy, mth, yr) = date.split('\\')
            if len(yr) != 4 or int(mth) > 12:
                (dy, mth, yr) = (False, False, False) 
        except:
            pass
    
    if not yr:
        try:
            (yr, mth) = date.split('-')
            dy = False
        except:
            pass
    
    if not yr:
        try:
            ldate = _getLabel(xml, filename)
            ldate = ldate.split(' ')[0]
            (yr, mth, dy) = ldate.split('-')
        except:
            pass

    if not yr or len(str(yr)) != 4:
        raise RuntimeError("Problem parsing this date: "+date)
    if not mth or int(mth) < 1 or int(mth) > 12:
        raise RuntimeError("Problem parsing this date: "+date)
    if dy and int(dy) > 31:
        raise RuntimeError("Problem parsing this date: "+date)
    if dy and int(dy) == 0:
        dy = None

    date = yr + '-' + mth
    date = date+'-'+dy if dy else date
    return date

def _getPDF(xml, filename):
    pdf = xml.findall('.//mets:fileGrp[@USE="pdf"]', ns)
    if pdf: 
        pdf = pdf[:-1]

    if not pdf:
        pdf = xml.find('.//mets:fileGrp[@USE="use_pdf"]', ns)

    if pdf:
        pdf = pdf.find('mets:file', ns)
        pdf = pdf.find('mets:FLocat', ns)
        
        pdffile = pdf.get('{http://www.w3.org/1999/xlink}href', False)
        if not pdffile:
            pdffile = pdf.get('{http://www.w3.org/TR/xlink}href', False)
        pdf = urlparse(pdffile)
        pdf = Path(str(pdf.netloc), str(pdf.path)).name
    
    if pdf is None or pdf == "":
        pdf = filename.replace('-METS.xml', '.pdf')

    return pdf

def _getLabel(xml, filename):
    xp = '//mets:div[@TYPE="issue" and descendant::mets:mptr[@*[.="%s"]]]' % filename
    issue = COLLECTION.xpath(xp, namespaces=ns)
    
    #Some issues aren't in the index.
    try:
        label = issue[0].get('LABEL')
    except:
        label = _getDate(xml, filename)

    #root = xml.getroot()
    #label = root.get('LABEL', False)
    if not label:
        raise RuntimeError("Can't find a label for "+metsfile+".")
    return label

def _getPages(xml, filename):
    filesec = xml.find('.//mets:fileSec', ns)
    jp2grp = filesec.find('mets:fileGrp[@USE="highres"]', ns)
    jpggrp = filesec.find('mets:fileGrp[@USE="lowres"]', ns)
    altogrp = filesec.find('mets:fileGrp[@USE="alto"]', ns)

    jp2s = jp2grp.findall('mets:file/mets:FLocat', ns)
    jpgs = jpggrp.findall('mets:file/mets:FLocat', ns)
    altos = altogrp.findall('mets:file/mets:FLocat', ns)

    if (len(jp2s) != len(jpgs)) or (len(jp2s) != len(altos)):
        raise RuntimeError("Page counts don't match.")
    
    outp = []
    for x in range(len(jp2s)):
        pn = x+1
        jp2name = jp2s[x].get('{http://www.w3.org/TR/xlink}href', False)
        if type(jp2name) == bool:
            jp2name = jp2s[x].get('{http://www.w3.org/1999/xlink}href', False)
        
        if type(jp2name) != bool:
            jp2 = Path(jp2name).name

        jpgname = jpgs[x].get('{http://www.w3.org/TR/xlink}href', False)
        if type(jpgname) == bool:
            jpgname = jpgs[x].get('{http://www.w3.org/1999/xlink}href', False)
        
        if type(jp2name) != bool:
            jpg = Path(jpgname).name

        if jpg == jp2:
            jpg = jpg.replace('.jp2', '.jpg')

        altoname = altos[x].get('{http://www.w3.org/TR/xlink}href', False)
        if type(altoname) == bool:
            altoname = altos[x].get('{http://www.w3.org/1999/xlink}href', False)
        
        if type(altoname) != bool:
            alto = Path(altoname).name
        
        #Storing altos raises unnecessary complications with pages with center spreads.
        #Deal with this later when setting the search engine up again.
        page = (pn, jp2, jpg)#, alto)

        if not all([not (x==False and type(x)==bool) for x in page]):
            print(page)
            raise RuntimeError("Missing some page data.")
        
        if x > 1 and jpg == outp[len(outp)-1][2]:
            #Don't insert this page. Instead, modify the last one so it has a dual page number.
            pagenum = str(pn-1) + "&ndash;" + str(pn)
            page = (pagenum,) + outp[len(outp) -1][1:]
            outp[len(outp)-1] = page
        else:
            outp.append(page)
    
    #if len(outp) != len(jp2s):
    #    raise RuntimeError("Wrong page count.")
    
    return outp


def parseMETS(metsfile):  
    print("parsing", metsfile)  
    p = Path(metsfile)
    with p.open("rb") as fl:
        filedata = fl.read()
        try: 
            xml = ET.fromstring(filedata)
        except ET.XMLSyntaxError: 
            print("THIS XML SUCKS")
            xmldat, tidyerr = tidy_document(filedata, options={'output-xml': 1, 'input-xml': 1, 'indent': 0, 'tidy-mark':0})
            xml = ET.fromstring(xmldat)

            with Path('./xmltemp.xml').open('wb') as newfl:
                newfl.write(xmldat)
    
    mets = Path(metsfile).name
    date = _getDate(xml, mets)
    label = _getLabel(xml, mets)
    pdf = _getPDF(xml, mets)

    data = (date, mets, pdf, label)
    if not all(date):
        raise RuntimeError("Something's missing in "+metsfile+"; collected data: "+data)

    #Some issues are missing pdfs. See 1279721237294622.xml and 1279721236576333.xml
    if not pdf.endswith('.pdf'):
        data = (date, mets, None, label)
    
    if '.pdf' == pdf:
        data = (date, mets, None, label)

    pages = _getPages(xml, mets)

    with db:
        try:
            c.execute(NEW_ISSUE, data)
            issueid = c.lastrowid
            pagecount = len(pages)
            pages = [(issueid,) + pg for pg in pages]
            allpages = tuple(pages)
            for pg in pages:
                c.execute(NEW_PAGE, pg)
            
            #Inserting one at a time makes it easier to bugfix.
            #c.executemany(NEW_PAGE, pages)
        except sqlite3.IntegrityError as e:
            #4578
            #First, find the issue the duplicate is from.
            print(data)
            print(list(pages))

            try:
                pg = pg
            except NameError:
                pg = pages[0]
            
            print("pg: ", pg)

            qry = '''SELECT issues.id, issues.mets, issues.label, COUNT(*) AS pages 
                        FROM issues JOIN pages ON issues.id = pages.issue_id
                        WHERE pages.issue_id = 
                            (SELECT issue_id FROM pages WHERE jpg = ?) 
                        GROUP BY issue_id;'''
            c.execute(qry, (pg[-1],))
            other = c.fetchone()
            #print(dict(other))
            print("This label:", xml.get('LABEL'), ";", label)
            #print("Other label", )

            otherid = int(other['id'])
            otherpcount = other['pages']

            pagematchcount = min((otherpcount, pagecount))
            
            otherpqry = "SELECT * FROM pages WHERE issue_id=? ORDER BY page_num"
            #print(type(otherpqry), type(otherid), otherid)
            c.execute(otherpqry, (otherid,))
            otherpages = c.fetchall()

            thisjpgs = [x[3] for x in allpages][:pagematchcount]
            otherjpgs = [y['jpg'] for y in otherpages][:pagematchcount]

            if thisjpgs != otherjpgs:
                #if any pages don't match, go ahead and fail.
                print("Non-matching pages.")
                raise e

            #for i in range(pagematchcount):
                #See if the existing pages match; if they do, dispose of the one with fewer pages.
                #if allpages[i][3] != otherpages[i]['jpg']
                    #print(allpages[i][3], otherpages[i]['jpg'])

                    #if any pages don't match, go ahead and fail.
                    #raise e
            
            if otherpcount <= pagecount:
                #Other should be ignored.
                #rename other. Add ".duplicate."+metsfile.replace('.xml', '') to the end.
                
                otherfile = Path(r'E:\\allmets', other['mets'])
                thisid = Path(metsfile).name.replace('.xml', '')
                otherfile.rename(str(otherfile)+'.duplicate.'+thisid)

                print(otherfile, thisid)

                #delete other from the database.
                c.execute('DELETE FROM pages WHERE issue_id = ?', (otherid, ))
                c.execute('DELETE FROM issues WHERE id = ?', (otherid, ))

                try:
                    #If we've inserted the issue already we need to remove it before re-adding.
                    c.execute('DELETE FROM issues WHERE id = ?', (issueid, ))
                except NameError:
                    pass

                db.commit()

                #print("rerun parsemets(metsfile); issue_id", otherid)
                parseMETS(metsfile)
                pass
            else:
                #the current file should be ignored.
                #rename metsfile.
                otherid = other['mets'].replace('.xml', '')
                Path(metsfile).rename(str(metsfile)+'.duplicate.'+otherid)
            

    print(data)


#TODO: cycle through the already-existing directory on Worf.
#TODO: cycle through directories like the groovy script does.

#parseMETS(r'E:\\allmets\\BDH_1905_02_25-METS.xml')
dr = Path(r'E:\\allmets')
files = dr.glob("*.xml")

i = 0
cnt = len(tuple(files))
files = dr.glob("*.xml")

#testfiles = ('E:\\allmets\\1279721240316399.xml', 'E:\\allmets\\127972123967327.xml')
testfiles = False

c.execute('DELETE FROM pages;')
c.execute('DELETE FROM issues;')
db.commit()

if testfiles:
    for tf in testfiles:
        parseMETS(tf)
else:
    for f in files:
        i += 1
        print ('\n(', i , " of ", cnt, ")")
        parseMETS(str(f))
        pass

#WORF.close()