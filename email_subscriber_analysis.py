#!/usr/bin/env python3
"""
Email Subscriber Analysis Script
Analyzes email subscriber data from CSV and produces a comprehensive report
"""

import csv
import re
from datetime import datetime, timedelta
from collections import defaultdict, Counter
from typing import Dict, List, Tuple, Set
import sys

# Email domain configurations
PROMINENT_EDU_EMAILS = {
    '@mit.edu', '@stanford.edu', '@berkeley.edu', '@columbia.edu',
    '@yale.edu', '@princeton.edu', '@harvard.edu', '@oxford.edu'
}

FORTUNE_100_EMAILS = {
    # Top 10
    '@walmart.com', '@amazon.com', '@exxonmobil.com', '@apple.com',
    '@uhg.com', '@unitedhealthgroup.com', '@cvshealth.com',
    '@brk.com', '@berkshirehathaway.com', '@google.com', '@alphabet.com',
    '@mckesson.com', '@chevron.com',
    # 11-50
    '@amerisourcebergen.com', '@costco.com', '@microsoft.com', '@cardinalhealth.com',
    '@cigna.com', '@marathonpetroleum.com', '@mpc.com', '@phillips66.com',
    '@valero.com', '@ford.com', '@homedepot.com', '@gm.com',
    '@elevancehealth.com', '@jpmorganchase.com', '@jpmorgan.com', '@kroger.com',
    '@centene.com', '@verizon.com', '@walgreens.com', '@wba.com',
    '@fanniemae.com', '@comcast.com', '@att.com', '@meta.com', '@facebook.com',
    '@bankofamerica.com', '@bofa.com', '@target.com', '@dell.com',
    '@adm.com', '@citigroup.com', '@citi.com', '@ups.com',
    '@pfizer.com', '@lowes.com', '@jnj.com', '@fedex.com',
    '@humana.com', '@energytransfer.com', '@statefarm.com',
    '@freddiemac.com', '@pepsico.com', '@wellsfargo.com',
    '@disney.com', '@conocophillips.com', '@tesla.com',
    # 51-100
    '@pg.com', '@ge.com', '@albertsons.com', '@metlife.com',
    '@gs.com', '@goldmansachs.com', '@sysco.com', '@rtx.com', '@raytheon.com',
    '@boeing.com', '@stonex.com', '@lockheedmartin.com', '@morganstanley.com',
    '@intel.com', '@hp.com', '@tdsynnex.com', '@ibm.com',
    '@hcahealthcare.com', '@prudential.com', '@caterpillar.com', '@cat.com',
    '@merck.com', '@wfscorp.com', '@newyorklife.com', '@epplp.com',
    '@abbvie.com', '@plainsallamerican.com', '@dow.com', '@aig.com',
    '@americanexpress.com', '@aexp.com', '@publix.com', '@charter.com',
    '@tyson.com', '@deere.com', '@cisco.com', '@nationwide.com',
    '@allstate.com', '@delta.com', '@libertymutual.com', '@tjx.com',
    '@progressive.com', '@aa.com', '@chsinc.com', '@pfgc.com',
    '@pbfenergy.com', '@nike.com', '@bestbuy.com', '@bms.com',
    '@united.com', '@thermofisher.com', '@qualcomm.com', '@abbott.com',
    '@coca-cola.com', '@coke.com'
}

# Company name mapping for cleaner output
COMPANY_NAME_MAPPING = {
    '@walmart.com': 'Walmart',
    '@amazon.com': 'Amazon',
    '@exxonmobil.com': 'Exxon Mobil',
    '@apple.com': 'Apple',
    '@uhg.com': 'UnitedHealth Group',
    '@unitedhealthgroup.com': 'UnitedHealth Group',
    '@cvshealth.com': 'CVS Health',
    '@brk.com': 'Berkshire Hathaway',
    '@berkshirehathaway.com': 'Berkshire Hathaway',
    '@google.com': 'Alphabet/Google',
    '@alphabet.com': 'Alphabet/Google',
    '@microsoft.com': 'Microsoft',
    '@meta.com': 'Meta/Facebook',
    '@facebook.com': 'Meta/Facebook',
    '@jpmorgan.com': 'JPMorgan Chase',
    '@jpmorganchase.com': 'JPMorgan Chase',
    '@bankofamerica.com': 'Bank of America',
    '@bofa.com': 'Bank of America',
    '@tesla.com': 'Tesla',
    '@disney.com': 'Walt Disney',
    '@intel.com': 'Intel',
    '@ibm.com': 'IBM',
    '@coca-cola.com': 'Coca-Cola',
    '@coke.com': 'Coca-Cola'
}

VC_STARTUP_EMAILS = {
    '@sequoiacap.com', '@a16z.com', '@accel.com', '@benchmark.com',
    '@bvp.com', '@khoslaventures.com', '@tigerglobal.com',
    '@openai.com', '@anthropic.com', '@palantir.com', '@stripe.com',
    '@coinbase.com', '@waymo.com'
}

PROMINENT_GOV_EMAILS = {
    'senate.gov', 'house.gov', 'state.gov', 'cbo.gov',
    'nasa.gov', 'doe.gov', 'nrel.gov', 'census.gov',
    'lbl.gov', 'nih.gov', 'nist.gov', 'sandia.gov'
}

STATE_GOV_EMAILS = {
    '@alabama.gov', '@alaska.gov', '@arizona.gov', '@arkansas.gov',
    '@ca.gov', '@colorado.gov', '@ct.gov', '@delaware.gov',
    '@florida.gov', '@georgia.gov', '@hawaii.gov', '@idaho.gov',
    '@illinois.gov', '@indiana.gov', '@iowa.gov', '@kansas.gov',
    '@kentucky.gov', '@louisiana.gov', '@maine.gov', '@maryland.gov',
    '@massachusetts.gov', '@michigan.gov', '@minnesota.gov', '@mississippi.gov',
    '@missouri.gov', '@montana.gov', '@nebraska.gov', '@nevada.gov',
    '@newhampshire.gov', '@nj.gov', '@newmexico.gov', '@ny.gov',
    '@nc.gov', '@nd.gov', '@ohio.gov', '@oklahoma.gov',
    '@oregon.gov', '@pa.gov', '@rhodeisland.gov', '@sc.gov',
    '@southdakota.gov', '@tennessee.gov', '@texas.gov', '@utah.gov',
    '@vermont.gov', '@virginia.gov', '@washington.gov', '@westvirginia.gov',
    '@wisconsin.gov', '@wyoming.gov', '@dc.gov', '@pr.gov',
    '@vi.gov', '@guam.gov', '@americansamoa.gov', '@cnmi.gov'
}

MEDIA_EMAILS = {
    '@wsj.com', '@ft.com', '@economist.com', '@nytimes.com',
    '@washpost.com', '@politico.com', '@bbc.co.uk', '@reuters.com',
    '@time.com', '@forbes.com', '@theatlantic.com', '@usatoday.com',
    '@latimes.com', '@ap.org', '@bloomberg.com', '@newyorker.com',
    '@wired.com', '@cnn.com', '@foxnews.com', '@msnbc.com',
    '@cnbc.com', '@npr.org', '@voxmedia.com', '@marketwatch.com'
}

MAJOR_PHILANTHROPY_EMAILS = {
    '@fordfoundation.org', '@arnoldventures.org', '@openphilanthropy.org',
    '@rand.org', '@rmi.org', '@gatesfoundation.org', '@rockefellerfoundation.org',
    '@macfound.org', '@chanzuckerberg.com', '@heritage.org',
    '@aei.org', '@cato.org', '@hoover.org', '@manhattan-institute.org',
    '@brookings.edu'
}

ALL_PHILANTHROPY_EMAILS = {
    # Major Philanthropic Foundations
    '@gatesfoundation.org', '@fordfoundation.org', '@rwjf.org', '@wkkf.org',
    '@rockefellerfoundation.org', '@carnegie.org', '@macfound.org', '@hewlett.org',
    '@bloomberg.org', '@waltonfamilyfoundation.org',
    # Education & Social Foundations
    '@aecf.org', '@luminafoundation.org', '@wallacefoundation.org', '@mott.org',
    '@lillyendowment.org', '@kresge.org', '@calendow.org', '@casey.org',
    # Newer Philanthropic Ventures
    '@arnoldventures.org', '@chanzuckerberg.com', '@opensocietyfoundations.org',
    '@knightfoundation.org', '@omidyar.com',
    # Think Tanks - Conservative/Right-Leaning
    '@heritage.org', '@aei.org', '@cato.org', '@hoover.org',
    '@hudson.org', '@manhattan-institute.org',
    # Think Tanks - Liberal/Progressive
    '@americanprogress.org', '@epi.org', '@cbpp.org', '@csis.org',
    '@newamerica.org',
    # Think Tanks - Nonpartisan/Research-Focused
    '@brookings.edu', '@rand.org', '@urban.org', '@carnegieendowment.org',
    '@cfr.org', '@piie.com', '@wilsoncenter.org', '@atlanticcouncil.org',
    # Specialized Policy Research
    '@rff.org', '@wri.org', '@cgdev.org', '@itif.org', '@gmfus.org',
    # Humanitarian & International Aid
    '@doctorswithoutborders.org', '@msf.org', '@redcross.org', '@oxfamamerica.org',
    '@savethechildren.org', '@worldvision.org', '@care.org', '@mercycorps.org',
    '@rescue.org',
    # Social Services & Community
    '@unitedway.org', '@salvationarmy.org', '@salvationarmyusa.org', '@goodwill.org',
    '@catholiccharitiesusa.org', '@feedingamerica.org', '@habitat.org',
    # Health Organizations
    '@cancer.org', '@heart.org', '@marchofdimes.org', '@stjude.org',
    '@plannedparenthood.org',
    # Environmental Organizations
    '@worldwildlife.org', '@tnc.org', '@edf.org', '@sierraclub.org',
    '@nrdc.org',
    # Human Rights & Advocacy
    '@aiusa.org', '@hrw.org', '@aclu.org', '@splcenter.org',
    '@freedomhouse.org',
    # Education & Youth
    '@bgca.org', '@girlscouts.org', '@scouting.org', '@bbbs.org',
    '@teachforamerica.org'
}


class EmailAnalyzer:
    def __init__(self, csv_file: str, years_back: int = 6):
        self.csv_file = csv_file
        self.years_back = years_back
        self.subscribers = []
        self.current_date = datetime.now()
        
    def load_data(self):
        """Load subscriber data from CSV file"""
        with open(self.csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                self.subscribers.append(row)
    
    def parse_date(self, date_str: str) -> datetime:
        """Parse date string to datetime object"""
        if not date_str or date_str.strip() == '':
            return None
        try:
            # Handle ISO format with timezone (e.g., 2020-09-27T22:51:49.282Z)
            if 'T' in date_str:
                # Remove timezone info if present
                date_str = date_str.split('.')[0].replace('T', ' ')
                if date_str.endswith('Z'):
                    date_str = date_str[:-1]
            
            # Try common date formats
            for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%m/%d/%Y', '%m/%d/%Y %H:%M:%S']:
                try:
                    return datetime.strptime(date_str.strip(), fmt)
                except ValueError:
                    continue
            return None
        except:
            return None
    
    def get_domain(self, email: str) -> str:
        """Extract domain from email address"""
        if '@' in email:
            return '@' + email.split('@')[1].lower()
        return ''
    
    def is_active(self, subscriber: dict) -> bool:
        """Check if subscriber is active (has opened an email)"""
        return bool(subscriber.get('Email last opened at', '').strip())
    
    def analyze_basic_stats(self) -> dict:
        """Analyze basic statistics"""
        total = len(self.subscribers)
        never_opened = sum(1 for s in self.subscribers if not self.is_active(s))
        
        # Calculate overall average open rate for last 6 months
        individual_open_rates = []
        all_subscriber_open_rates = []
        active_with_emails_open_rates = []
        
        for subscriber in self.subscribers:
            emails_received = int(subscriber.get('Email receives (last 6 months)', 0) or 0)
            emails_opened = int(subscriber.get('Emails opened (last 6 months)', 0) or 0)
            is_active = self.is_active(subscriber)
            
            if emails_received > 0:
                open_rate = (emails_opened / emails_received) * 100
                individual_open_rates.append(open_rate)
                all_subscriber_open_rates.append(open_rate)
                
                # Only include if subscriber has ever opened an email
                if is_active:
                    active_with_emails_open_rates.append(open_rate)
            else:
                # For subscribers with 0 emails received, count as 0% open rate
                all_subscriber_open_rates.append(0.0)
        
        avg_open_rate = sum(individual_open_rates) / len(individual_open_rates) if individual_open_rates else 0
        avg_open_rate_all = sum(all_subscriber_open_rates) / len(all_subscriber_open_rates) if all_subscriber_open_rates else 0
        avg_open_rate_active_with_emails = sum(active_with_emails_open_rates) / len(active_with_emails_open_rates) if active_with_emails_open_rates else 0
        
        return {
            'total_subscribers': total,
            'never_opened': never_opened,
            'never_opened_fraction': never_opened / total if total > 0 else 0,
            'avg_open_rate': avg_open_rate,
            'avg_open_rate_all': avg_open_rate_all,
            'avg_open_rate_active_with_emails': avg_open_rate_active_with_emails,
            'subscribers_with_emails': len(individual_open_rates),
            'active_subscribers_with_emails': len(active_with_emails_open_rates)
        }
    
    def analyze_subscription_age(self) -> dict:
        """Analyze subscription age histogram"""
        age_buckets = defaultdict(int)
        
        for subscriber in self.subscribers:
            created_date = self.parse_date(subscriber.get('Subscription date', ''))
            if created_date:
                age_days = (self.current_date - created_date).days
                # Skip future dates
                if age_days < 0:
                    continue
                age_months = age_days / 30
                
                # Create buckets for every 6 months up to years_back years
                bucket_index = min(int(age_months // 6), self.years_back * 2 - 1)
                age_buckets[bucket_index] += 1
        
        return dict(age_buckets)
    
    def analyze_open_rates(self) -> dict:
        """Analyze email open rates for last 6 months"""
        open_rate_buckets = defaultdict(int)
        zero_email_receives = 0
        
        for subscriber in self.subscribers:
            # Get emails received and opened in last 6 months
            emails_received = int(subscriber.get('Email receives (last 6 months)', 0) or 0)
            emails_opened = int(subscriber.get('Emails opened (last 6 months)', 0) or 0)
            
            if emails_received > 0:
                open_rate = (emails_opened / emails_received) * 100
                bucket = min(int(open_rate // 10), 9)  # 0-9 for 0-100%
                open_rate_buckets[bucket] += 1
            else:
                zero_email_receives += 1
        
        result = dict(open_rate_buckets)
        result['zero_receives'] = zero_email_receives
        return result
    
    def analyze_open_rates_by_age(self) -> dict:
        """Analyze average email open rates by subscription age"""
        age_open_stats = defaultdict(lambda: {'open_rates': [], 'subscriber_count': 0})
        
        for subscriber in self.subscribers:
            created_date = self.parse_date(subscriber.get('Subscription date', ''))
            if not created_date:
                continue
                
            age_days = (self.current_date - created_date).days
            # Skip future dates
            if age_days < 0:
                continue
            age_months = age_days / 30
            
            # Create buckets for every 6 months up to years_back years
            bucket_index = min(int(age_months // 6), self.years_back * 2 - 1)
            
            # Get emails received and opened in last 6 months
            emails_received = int(subscriber.get('Email receives (last 6 months)', 0) or 0)
            emails_opened = int(subscriber.get('Emails opened (last 6 months)', 0) or 0)
            
            # Calculate individual open rate
            if emails_received > 0:
                individual_open_rate = (emails_opened / emails_received) * 100
                age_open_stats[bucket_index]['open_rates'].append(individual_open_rate)
            
            age_open_stats[bucket_index]['subscriber_count'] += 1
        
        # Calculate average of individual open rates for each age bucket
        result = {}
        for bucket, stats in age_open_stats.items():
            if stats['open_rates']:
                # True average: mean of individual open rates
                avg_open_rate = sum(stats['open_rates']) / len(stats['open_rates'])
            else:
                avg_open_rate = 0
            result[bucket] = {
                'avg_open_rate': avg_open_rate,
                'subscriber_count': stats['subscriber_count']
            }
        
        return result
    
    def analyze_open_rates_by_age_all(self) -> dict:
        """Analyze average email open rates by subscription age (including zero email receives)"""
        age_open_stats = defaultdict(lambda: {'open_rates': [], 'subscriber_count': 0})
        
        for subscriber in self.subscribers:
            created_date = self.parse_date(subscriber.get('Subscription date', ''))
            if not created_date:
                continue
                
            age_days = (self.current_date - created_date).days
            # Skip future dates
            if age_days < 0:
                continue
            age_months = age_days / 30
            
            # Create buckets for every 6 months up to years_back years
            bucket_index = min(int(age_months // 6), self.years_back * 2 - 1)
            
            # Get emails received and opened in last 6 months
            emails_received = int(subscriber.get('Email receives (last 6 months)', 0) or 0)
            emails_opened = int(subscriber.get('Emails opened (last 6 months)', 0) or 0)
            
            # Calculate individual open rate (including 0% for zero receives)
            if emails_received > 0:
                individual_open_rate = (emails_opened / emails_received) * 100
            else:
                individual_open_rate = 0.0
            
            age_open_stats[bucket_index]['open_rates'].append(individual_open_rate)
            age_open_stats[bucket_index]['subscriber_count'] += 1
        
        # Calculate average of individual open rates for each age bucket
        result = {}
        for bucket, stats in age_open_stats.items():
            if stats['open_rates']:
                # True average: mean of individual open rates (including zeros)
                avg_open_rate = sum(stats['open_rates']) / len(stats['open_rates'])
            else:
                avg_open_rate = 0
            result[bucket] = {
                'avg_open_rate': avg_open_rate,
                'subscriber_count': stats['subscriber_count']
            }
        
        return result
    
    def analyze_zero_receives_by_age(self) -> dict:
        """Analyze percentage of subscribers with 0 email receives by subscription age"""
        age_stats = defaultdict(lambda: {'total': 0, 'zero_receives': 0})
        
        for subscriber in self.subscribers:
            created_date = self.parse_date(subscriber.get('Subscription date', ''))
            if not created_date:
                continue
                
            age_days = (self.current_date - created_date).days
            # Skip future dates
            if age_days < 0:
                continue
            age_months = age_days / 30
            
            # Create buckets for every 6 months up to years_back years
            bucket_index = min(int(age_months // 6), self.years_back * 2 - 1)
            
            # Get emails received in last 6 months
            emails_received = int(subscriber.get('Email receives (last 6 months)', 0) or 0)
            
            age_stats[bucket_index]['total'] += 1
            if emails_received == 0:
                age_stats[bucket_index]['zero_receives'] += 1
        
        # Calculate percentage for each age bucket
        result = {}
        for bucket, stats in age_stats.items():
            if stats['total'] > 0:
                zero_percent = (stats['zero_receives'] / stats['total']) * 100
            else:
                zero_percent = 0
            result[bucket] = {
                'zero_percent': zero_percent,
                'zero_count': stats['zero_receives'],
                'total_count': stats['total']
            }
        
        return result
    
    def analyze_edu_emails(self) -> dict:
        """Analyze .edu email addresses"""
        edu_stats = {
            'total': 0,
            'active': 0,
            'prominent': {'total': 0, 'active': 0, 'by_domain': {}},
            'top_10': Counter(),
            'top_10_active': Counter()
        }
        
        # Initialize prominent domains
        for domain in PROMINENT_EDU_EMAILS:
            edu_stats['prominent']['by_domain'][domain] = {'total': 0, 'active': 0}
        
        for subscriber in self.subscribers:
            email = subscriber.get('Email', '').lower()
            domain = self.get_domain(email)
            
            if domain.endswith('.edu'):
                edu_stats['total'] += 1
                if self.is_active(subscriber):
                    edu_stats['active'] += 1
                    edu_stats['top_10_active'][domain] += 1
                edu_stats['top_10'][domain] += 1
                
                if domain in PROMINENT_EDU_EMAILS:
                    edu_stats['prominent']['total'] += 1
                    edu_stats['prominent']['by_domain'][domain]['total'] += 1
                    if self.is_active(subscriber):
                        edu_stats['prominent']['active'] += 1
                        edu_stats['prominent']['by_domain'][domain]['active'] += 1
        
        # Get top 10
        edu_stats['top_10'] = dict(edu_stats['top_10'].most_common(10))
        edu_stats['top_10_active'] = dict(edu_stats['top_10_active'].most_common(10))
        
        return edu_stats
    
    def analyze_corporation_emails(self) -> dict:
        """Analyze major corporation emails"""
        corp_stats = {
            'total': 0,
            'active': 0,
            'by_company': defaultdict(lambda: {'total': 0, 'active': 0})
        }
        
        for subscriber in self.subscribers:
            email = subscriber.get('Email', '').lower()
            domain = self.get_domain(email)
            
            if domain in FORTUNE_100_EMAILS:
                corp_stats['total'] += 1
                
                # Get company name
                company_name = COMPANY_NAME_MAPPING.get(domain, domain)
                corp_stats['by_company'][company_name]['total'] += 1
                
                if self.is_active(subscriber):
                    corp_stats['active'] += 1
                    corp_stats['by_company'][company_name]['active'] += 1
        
        # Get top 10 companies
        top_companies = sorted(
            corp_stats['by_company'].items(),
            key=lambda x: x[1]['total'],
            reverse=True
        )[:10]
        
        corp_stats['top_10'] = dict(top_companies)
        del corp_stats['by_company']  # Remove full list to keep output clean
        
        return corp_stats
    
    def analyze_vc_startup_emails(self) -> dict:
        """Analyze VC and startup emails"""
        vc_stats = {'total': 0, 'active': 0, 'by_domain': {}}
        
        for subscriber in self.subscribers:
            email = subscriber.get('Email', '').lower()
            domain = self.get_domain(email)
            
            if domain in VC_STARTUP_EMAILS:
                vc_stats['total'] += 1
                if domain not in vc_stats['by_domain']:
                    vc_stats['by_domain'][domain] = {'total': 0, 'active': 0}
                vc_stats['by_domain'][domain]['total'] += 1
                
                if self.is_active(subscriber):
                    vc_stats['active'] += 1
                    vc_stats['by_domain'][domain]['active'] += 1
        
        return vc_stats
    
    def analyze_government_emails(self) -> dict:
        """Analyze government emails"""
        gov_stats = {
            'total': 0,
            'active': 0,
            'top_10_domains': Counter(),
            'prominent': {'total': 0, 'active': 0, 'by_domain': {}},
            'states': {'total': 0, 'active': 0}
        }
        
        for subscriber in self.subscribers:
            email = subscriber.get('Email', '').lower()
            domain = self.get_domain(email)
            
            if '.gov' in domain:
                gov_stats['total'] += 1
                gov_stats['top_10_domains'][domain] += 1
                
                if self.is_active(subscriber):
                    gov_stats['active'] += 1
                
                # Check prominent domains
                for prom_domain in PROMINENT_GOV_EMAILS:
                    if prom_domain in domain:
                        gov_stats['prominent']['total'] += 1
                        if prom_domain not in gov_stats['prominent']['by_domain']:
                            gov_stats['prominent']['by_domain'][prom_domain] = {'total': 0, 'active': 0}
                        gov_stats['prominent']['by_domain'][prom_domain]['total'] += 1
                        
                        if self.is_active(subscriber):
                            gov_stats['prominent']['active'] += 1
                            gov_stats['prominent']['by_domain'][prom_domain]['active'] += 1
                
                # Check state domains
                if domain in STATE_GOV_EMAILS:
                    gov_stats['states']['total'] += 1
                    if self.is_active(subscriber):
                        gov_stats['states']['active'] += 1
        
        gov_stats['top_10_domains'] = dict(gov_stats['top_10_domains'].most_common(10))
        
        return gov_stats
    
    def analyze_media_emails(self) -> dict:
        """Analyze media organization emails"""
        media_stats = {
            'total': 0,
            'active': 0,
            'by_outlet': {}
        }
        
        for outlet in MEDIA_EMAILS:
            media_stats['by_outlet'][outlet] = {'total': 0, 'active': 0}
        
        for subscriber in self.subscribers:
            email = subscriber.get('Email', '').lower()
            domain = self.get_domain(email)
            
            if domain in MEDIA_EMAILS:
                media_stats['total'] += 1
                media_stats['by_outlet'][domain]['total'] += 1
                
                if self.is_active(subscriber):
                    media_stats['active'] += 1
                    media_stats['by_outlet'][domain]['active'] += 1
        
        # Remove outlets with 0 subscribers for cleaner output
        media_stats['by_outlet'] = {k: v for k, v in media_stats['by_outlet'].items() 
                                   if v['total'] > 0}
        
        return media_stats
    
    def analyze_org_emails(self) -> dict:
        """Analyze .org emails including philanthropy, nonprofits, and think tanks"""
        org_stats = {
            'top_10_org': Counter(),
            'major_orgs': {},
            'all_philanthropy': {'total': 0, 'active': 0}
        }
        
        # Initialize major orgs
        for org in MAJOR_PHILANTHROPY_EMAILS:
            org_stats['major_orgs'][org] = {'total': 0, 'active': 0}
        
        for subscriber in self.subscribers:
            email = subscriber.get('Email', '').lower()
            domain = self.get_domain(email)
            
            # Count all .org domains
            if domain.endswith('.org'):
                org_stats['top_10_org'][domain] += 1
            
            # Check major organizations
            if domain in MAJOR_PHILANTHROPY_EMAILS:
                org_stats['major_orgs'][domain]['total'] += 1
                if self.is_active(subscriber):
                    org_stats['major_orgs'][domain]['active'] += 1
            
            # Check all philanthropy organizations
            if domain in ALL_PHILANTHROPY_EMAILS:
                org_stats['all_philanthropy']['total'] += 1
                if self.is_active(subscriber):
                    org_stats['all_philanthropy']['active'] += 1
        
        # Get top 10 .org domains
        org_stats['top_10_org'] = dict(org_stats['top_10_org'].most_common(10))
        
        # Remove orgs with 0 subscribers
        org_stats['major_orgs'] = {k: v for k, v in org_stats['major_orgs'].items() 
                                  if v['total'] > 0}
        
        return org_stats
    
    def generate_report(self, output_file: str):
        """Generate the complete analysis report"""
        print("Loading data...")
        self.load_data()
        
        print("Analyzing data...")
        report = []
        report.append("EMAIL SUBSCRIBER ANALYSIS REPORT")
        report.append("=" * 60)
        report.append(f"Analysis Date: {self.current_date.strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Data File: {self.csv_file}")
        report.append("")
        
        # Basic statistics
        print("Analyzing basic statistics...")
        basic_stats = self.analyze_basic_stats()
        report.append("BASIC STATISTICS")
        report.append("-" * 40)
        report.append(f"Total Subscribers: {basic_stats['total_subscribers']:,}")
        report.append(f"Never Opened Email: {basic_stats['never_opened']:,} ({basic_stats['never_opened_fraction']:.1%})")
        report.append(f"Average Open Rate (last 6 months): {basic_stats['avg_open_rate']:.1f}%")
        report.append(f"Average Open Rate (including zero email receives): {basic_stats['avg_open_rate_all']:.1f}%")
        report.append(f"Average Open Rate (active subscribers with emails): {basic_stats['avg_open_rate_active_with_emails']:.1f}%")
        report.append("")
        
        # Subscription age histogram
        print("Analyzing subscription age...")
        age_hist = self.analyze_subscription_age()
        report.append("SUBSCRIPTION AGE HISTOGRAM (6-month increments)")
        report.append("-" * 40)
        
        # Find max value for scaling bars
        max_age_value = max(age_hist.values()) if age_hist else 1
        bar_width = 40  # Maximum bar width in characters
        
        for bucket in sorted(age_hist.keys()):
            start_months = bucket * 6
            end_months = (bucket + 1) * 6
            count = age_hist[bucket]
            percentage = (count / basic_stats['total_subscribers']) * 100
            
            # Create bar
            bar_length = int((count / max_age_value) * bar_width)
            bar = '█' * bar_length
            
            report.append(f"{start_months:3d}-{end_months:<3d} months: {count:6,} ({percentage:4.1f}%) {bar}")
        report.append("")
        
        # Open rate histogram
        print("Analyzing open rates...")
        open_rates = self.analyze_open_rates()
        report.append("EMAIL OPEN RATE HISTOGRAM (Last 6 months)")
        report.append("-" * 40)
        
        # Separate zero receives from the regular buckets
        zero_receives = open_rates.pop('zero_receives', 0)
        
        # Find max value for scaling bars (including zero receives in comparison)
        all_values = list(open_rates.values()) + [zero_receives]
        max_open_value = max(all_values) if all_values else 1
        
        # Calculate total for percentages (including zero receives)
        total_in_histogram = sum(open_rates.values()) + zero_receives
        
        for bucket in sorted(open_rates.keys(), reverse=True):
            start_pct = bucket * 10
            end_pct = (bucket + 1) * 10
            count = open_rates[bucket]
            percentage = (count / total_in_histogram) * 100 if total_in_histogram > 0 else 0
            
            # Create bar
            bar_length = int((count / max_open_value) * bar_width)
            bar = '█' * bar_length
            
            report.append(f"{start_pct:2d}-{end_pct:<3d}% open rate: {count:6,} ({percentage:4.1f}%) {bar}")
        
        # Add zero receives line
        if zero_receives > 0:
            percentage = (zero_receives / total_in_histogram) * 100 if total_in_histogram > 0 else 0
            bar_length = int((zero_receives / max_open_value) * bar_width)
            bar = '█' * bar_length
            report.append(f"     0 email receives: {zero_receives:6,} ({percentage:4.1f}%) {bar}")
        
        report.append("")
        
        # Average open rate by age histogram
        print("Analyzing open rates by age...")
        age_open_rates = self.analyze_open_rates_by_age()
        report.append("AVERAGE OPEN RATE BY SUBSCRIPTION AGE")
        report.append("-" * 40)
        
        # Find max value for scaling bars (using 100% as max since these are percentages)
        max_rate = 100
        
        for bucket in sorted(age_open_rates.keys()):
            start_months = bucket * 6
            end_months = (bucket + 1) * 6
            stats = age_open_rates[bucket]
            avg_rate = stats['avg_open_rate']
            count = stats['subscriber_count']
            
            # Create bar based on percentage
            bar_length = int((avg_rate / max_rate) * bar_width)
            bar = '█' * bar_length
            
            report.append(f"{start_months:3d}-{end_months:<3d} months: {avg_rate:5.1f}% avg open rate ({count:>6,} subscribers) {bar}")
        report.append("")
        
        # Average open rate by age histogram (including zero receives)
        print("Analyzing open rates by age (including zero receives)...")
        age_open_rates_all = self.analyze_open_rates_by_age_all()
        report.append("AVERAGE OPEN RATE BY SUBSCRIPTION AGE (including zero email receives)")
        report.append("-" * 40)
        
        for bucket in sorted(age_open_rates_all.keys()):
            start_months = bucket * 6
            end_months = (bucket + 1) * 6
            stats = age_open_rates_all[bucket]
            avg_rate = stats['avg_open_rate']
            count = stats['subscriber_count']
            
            # Create bar based on percentage
            bar_length = int((avg_rate / max_rate) * bar_width)
            bar = '█' * bar_length
            
            report.append(f"{start_months:3d}-{end_months:<3d} months: {avg_rate:5.1f}% avg open rate ({count:>6,} subscribers) {bar}")
        report.append("")
        
        # Zero email receives by age histogram
        print("Analyzing zero email receives by age...")
        zero_by_age = self.analyze_zero_receives_by_age()
        report.append("PERCENT WITH 0 EMAIL RECEIVES BY SUBSCRIPTION AGE")
        report.append("-" * 40)
        
        for bucket in sorted(zero_by_age.keys()):
            start_months = bucket * 6
            end_months = (bucket + 1) * 6
            stats = zero_by_age[bucket]
            zero_percent = stats['zero_percent']
            zero_count = stats['zero_count']
            total_count = stats['total_count']
            
            # Create bar based on percentage
            bar_length = int((zero_percent / 100) * bar_width)
            bar = '█' * bar_length
            
            report.append(f"{start_months:3d}-{end_months:<3d} months: {zero_percent:5.1f}% have 0 receives ({zero_count:>5,}/{total_count:,}) {bar}")
        report.append("")
        
        # .edu emails
        print("Analyzing .edu emails...")
        edu_stats = self.analyze_edu_emails()
        report.append(".EDU EMAIL ANALYSIS")
        report.append("-" * 40)
        report.append(f"Total .edu subscribers: {edu_stats['total']:,}")
        report.append(f"Active .edu subscribers: {edu_stats['active']:,}")
        report.append(f"Prominent .edu subscribers (total): {edu_stats['prominent']['total']:,} (active: {edu_stats['prominent']['active']:,})")
        report.append("\nTop 10 .edu domains:")
        for domain, count in edu_stats['top_10'].items():
            active_count = edu_stats['top_10_active'].get(domain, 0)
            report.append(f"  {domain}: {count} (active: {active_count})")
        report.append("\nProminent .edu domains breakdown:")
        for domain in ['@mit.edu', '@stanford.edu', '@berkeley.edu', '@columbia.edu', 
                      '@yale.edu', '@princeton.edu', '@harvard.edu', '@oxford.edu']:
            stats = edu_stats['prominent']['by_domain'].get(domain, {'total': 0, 'active': 0})
            report.append(f"  {domain}: {stats['total']} (active: {stats['active']})")
        report.append("")
        
        # Major corporations
        print("Analyzing corporation emails...")
        corp_stats = self.analyze_corporation_emails()
        report.append("MAJOR CORPORATION EMAIL ANALYSIS")
        report.append("-" * 40)
        report.append(f"Total Fortune 100 subscribers: {corp_stats['total']:,}")
        report.append(f"Active Fortune 100 subscribers: {corp_stats['active']:,}")
        report.append("\nTop 10 corporations:")
        for company, stats in corp_stats['top_10'].items():
            report.append(f"  {company}: {stats['total']} (active: {stats['active']})")
        report.append("")
        
        # VC and startups
        print("Analyzing VC/startup emails...")
        vc_stats = self.analyze_vc_startup_emails()
        report.append("VC AND STARTUP EMAIL ANALYSIS")
        report.append("-" * 40)
        report.append(f"Total VC/startup subscribers: {vc_stats['total']:,}")
        report.append(f"Active VC/startup subscribers: {vc_stats['active']:,}")
        if vc_stats['by_domain']:
            report.append("\nBy domain:")
            for domain, stats in sorted(vc_stats['by_domain'].items()):
                report.append(f"  {domain}: {stats['total']} (active: {stats['active']})")
        report.append("")
        
        # Government emails
        print("Analyzing government emails...")
        gov_stats = self.analyze_government_emails()
        report.append("GOVERNMENT EMAIL ANALYSIS")
        report.append("-" * 40)
        report.append(f"Total .gov subscribers: {gov_stats['total']:,}")
        report.append(f"Active .gov subscribers: {gov_stats['active']:,}")
        report.append(f"State .gov subscribers: {gov_stats['states']['total']:,} (active: {gov_stats['states']['active']:,})")
        report.append(f"Prominent .gov subscribers: {gov_stats['prominent']['total']:,} (active: {gov_stats['prominent']['active']:,})")
        report.append("\nTop 10 .gov domains:")
        for domain, count in gov_stats['top_10_domains'].items():
            report.append(f"  {domain}: {count}")
        if gov_stats['prominent']['by_domain']:
            report.append("\nProminent .gov domains:")
            for domain, stats in sorted(gov_stats['prominent']['by_domain'].items()):
                report.append(f"  {domain}: {stats['total']} (active: {stats['active']})")
        report.append("")
        
        # Media emails
        print("Analyzing media emails...")
        media_stats = self.analyze_media_emails()
        report.append("MEDIA EMAIL ANALYSIS")
        report.append("-" * 40)
        report.append(f"Total media subscribers: {media_stats['total']:,}")
        report.append(f"Active media subscribers: {media_stats['active']:,}")
        if media_stats['by_outlet']:
            report.append("\nBy outlet:")
            for outlet, stats in sorted(media_stats['by_outlet'].items()):
                report.append(f"  {outlet}: {stats['total']} (active: {stats['active']})")
        report.append("")
        
        # Philanthropy/nonprofit/think tank emails
        print("Analyzing philanthropy/nonprofit emails...")
        org_stats = self.analyze_org_emails()
        report.append("PHILANTHROPY, NONPROFIT, AND THINK TANK ANALYSIS")
        report.append("-" * 40)
        report.append(f"Total from all tracked philanthropy orgs: {org_stats['all_philanthropy']['total']:,}")
        report.append(f"Active from all tracked philanthropy orgs: {org_stats['all_philanthropy']['active']:,}")
        report.append("\nTop 10 .org domains:")
        for domain, count in org_stats['top_10_org'].items():
            report.append(f"  {domain}: {count}")
        if org_stats['major_orgs']:
            report.append("\nMajor organizations:")
            for org, stats in sorted(org_stats['major_orgs'].items()):
                report.append(f"  {org}: {stats['total']} (active: {stats['active']})")
        report.append("")
        
        # Write report to file
        print(f"Writing report to {output_file}...")
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report))
        
        print("Analysis complete!")
        return '\n'.join(report[:50]) + '\n...\n[Report continues in output file]'


def main():
    # Configuration
    csv_file = "full_email.csv"
    output_file = "email_analysis_report.txt"
    years_back = 6  # Configurable parameter
    
    # Run analysis
    analyzer = EmailAnalyzer(csv_file, years_back)
    try:
        analyzer.generate_report(output_file)
        print(f"\nReport saved to: {output_file}")
    except FileNotFoundError:
        print(f"Error: Could not find the file '{csv_file}'. Please ensure it exists in the current directory.")
        sys.exit(1)
    except Exception as e:
        print(f"Error during analysis: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()