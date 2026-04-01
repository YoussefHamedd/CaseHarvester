"""
Parser for new MJCS React SPA JSON format (detail_loc = MJCS2).

Handles:
  - caseDetail  (standard: CR, CV, FAM, traffic, etc.)
  - dvCaseDTO   (Domestic Violence cases)

Populates tables:
  mjcs2, mjcs2_defendants, mjcs2_involved_parties, mjcs2_attorneys,
  mjcs2_charges, mjcs2_hearings, mjcs2_events, mjcs2_judgments,
  mjcs2_service_events, mjcs2_causes
"""
import json
import logging
from datetime import datetime
from sqlalchemy import text
from ..util import db_session
from ..models import Case
from sqlalchemy import update
from . import ParserError

logger = logging.getLogger('mjcs')


def _parse_date(s):
    if not s:
        return None
    s = str(s).strip()
    for fmt in ('%m/%d/%Y', '%Y-%m-%d'):
        try:
            return datetime.strptime(s[:10], fmt).date()
        except Exception:
            pass
    return None


def _str(v):
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def _db_write(case_number, main, defendants, parties, attorneys,
              charges, hearings, events, judgments, service_events, causes):
    with db_session() as db:
        # ── mjcs2 main ──────────────────────────────────────────────
        db.execute(text("""
            INSERT INTO mjcs2 (case_number, internal_id, court_system, case_category,
                case_type, case_title, filing_date, filing_date_str, case_status,
                case_status_date, court_name, judge_assigned,
                charge_track_number, violation_date, violation_county)
            VALUES (:case_number, :internal_id, :court_system, :case_category,
                :case_type, :case_title, :filing_date, :filing_date_str, :case_status,
                :case_status_date, :court_name, :judge_assigned,
                :charge_track_number, :violation_date, :violation_county)
            ON CONFLICT (case_number) DO UPDATE SET
                internal_id         = EXCLUDED.internal_id,
                court_system        = EXCLUDED.court_system,
                case_category       = EXCLUDED.case_category,
                case_type           = EXCLUDED.case_type,
                case_title          = EXCLUDED.case_title,
                filing_date         = EXCLUDED.filing_date,
                filing_date_str     = EXCLUDED.filing_date_str,
                case_status         = EXCLUDED.case_status,
                case_status_date    = EXCLUDED.case_status_date,
                court_name          = EXCLUDED.court_name,
                judge_assigned      = EXCLUDED.judge_assigned,
                charge_track_number = EXCLUDED.charge_track_number,
                violation_date      = EXCLUDED.violation_date,
                violation_county    = EXCLUDED.violation_county
        """), main)

        # ── clear child rows then re-insert ─────────────────────────
        for tbl in ('mjcs2_defendants', 'mjcs2_involved_parties',
                    'mjcs2_attorneys', 'mjcs2_charges', 'mjcs2_hearings',
                    'mjcs2_events', 'mjcs2_judgments',
                    'mjcs2_service_events', 'mjcs2_causes'):
            db.execute(text(f"DELETE FROM {tbl} WHERE case_number = :cn"),
                       {'cn': case_number})

        if defendants:
            db.execute(text("""
                INSERT INTO mjcs2_defendants
                    (case_number, defendant_name, race, gender, dob,
                     height_feet, height_inches, weight,
                     address_line1, address_line2, city, state, zip, current_address)
                VALUES (:case_number, :defendant_name, :race, :gender, :dob,
                        :height_feet, :height_inches, :weight,
                        :address_line1, :address_line2, :city, :state, :zip, :current_address)
            """), defendants)

        if parties:
            db.execute(text("""
                INSERT INTO mjcs2_involved_parties
                    (case_number, party_type, party_name,
                     address_line1, address_line2, city, state, zip)
                VALUES (:case_number, :party_type, :party_name,
                        :address_line1, :address_line2, :city, :state, :zip)
            """), parties)

        if attorneys:
            db.execute(text("""
                INSERT INTO mjcs2_attorneys
                    (case_number, party_name, attorney_name, appearance_date,
                     address_line1, city, state, zip)
                VALUES (:case_number, :party_name, :attorney_name, :appearance_date,
                        :address_line1, :city, :state, :zip)
            """), attorneys)

        if charges:
            db.execute(text("""
                INSERT INTO mjcs2_charges
                    (case_number, charge_number, statute_code, charge_description,
                     offense_date, fine_amount, officer_name, agency_name,
                     vehicle_tag, vehicle_desc, recorded_speed, speed_limit,
                     mandatory_court, probable_cause, disposition)
                VALUES (:case_number, :charge_number, :statute_code, :charge_description,
                        :offense_date, :fine_amount, :officer_name, :agency_name,
                        :vehicle_tag, :vehicle_desc, :recorded_speed, :speed_limit,
                        :mandatory_court, :probable_cause, :disposition)
            """), charges)

        if hearings:
            db.execute(text("""
                INSERT INTO mjcs2_hearings
                    (case_number, event_type, event_date, event_time,
                     location, room, result, judge)
                VALUES (:case_number, :event_type, :event_date, :event_time,
                        :location, :room, :result, :judge)
            """), hearings)

        if events:
            db.execute(text("""
                INSERT INTO mjcs2_events
                    (case_number, file_date, document_name, internal_event_id)
                VALUES (:case_number, :file_date, :document_name, :internal_event_id)
            """), events)

        if judgments:
            db.execute(text("""
                INSERT INTO mjcs2_judgments
                    (case_number, judgment_type, issue_date, judge)
                VALUES (:case_number, :judgment_type, :issue_date, :judge)
            """), judgments)

        if service_events:
            db.execute(text("""
                INSERT INTO mjcs2_service_events (case_number, service_type, issue_date)
                VALUES (:case_number, :service_type, :issue_date)
            """), service_events)

        if causes:
            db.execute(text("""
                INSERT INTO mjcs2_causes
                    (case_number, file_date, filed_by, filed_against,
                     cause_description, remedy, remedy_amount, remedy_comment)
                VALUES (:case_number, :file_date, :filed_by, :filed_against,
                        :cause_description, :remedy, :remedy_amount, :remedy_comment)
            """), causes)

        # Mark parsed
        db.execute(
            update(Case)
            .filter_by(case_number=case_number)
            .values(last_parse=datetime.utcnow(), detail_loc='MJCS2')
        )


class MJCS2Parser:
    """Parses JSON saved by the new React SPA scraper into mjcs2_* tables."""

    def __init__(self, case_number, case_json):
        self.case_number = case_number
        self.data = json.loads(case_json) if isinstance(case_json, (bytes, str)) else case_json

    def parse(self):
        if 'dvCaseDTO' in self.data:
            self._parse_dv(self.data['dvCaseDTO'])
        elif 'caseDetail' in self.data:
            self._parse_standard(self.data['caseDetail'])
        else:
            key = list(self.data.keys())[0] if self.data else 'empty'
            raise ParserError(f'Unknown JSON root key "{key}" for {self.case_number}')

    # ══════════════════════════════════════════════════════════════
    # STANDARD caseDetail format
    # ══════════════════════════════════════════════════════════════
    def _parse_standard(self, d):
        cn = self.case_number
        filing_date_str = _str(d.get('filedDate', ''))
        status_obj  = d.get('caseStatus') or {}
        court_obj   = d.get('court') or {}

        main = {
            'case_number':       cn,
            'internal_id':       _str(d.get('internalId')),
            'court_system':      _str(d.get('courtSystem')),
            'case_category':     _str(d.get('caseCategory')),
            'case_type':         _str(d.get('caseType')),
            'case_title':        _str(d.get('caseTitle')),
            'filing_date':       _parse_date(filing_date_str),
            'filing_date_str':   filing_date_str,
            'case_status':       _str(status_obj.get('caseStatusType')),
            'case_status_date':  _parse_date(status_obj.get('date')),
            'court_name':        _str(court_obj.get('courtName')),
            'judge_assigned':    _str(d.get('judgeAssigned')),
            'charge_track_number': _str(d.get('chargeTrackNumber')),
            'violation_date':    _parse_date(d.get('voilationDate')),   # typo in API
            'violation_county':  _str(d.get('voilationCounty')),
        }

        # ── Defendants ──────────────────────────────────────────────
        defendants = []
        for def_ in (d.get('defendentInfo') or []):
            addrs = def_.get('defendantAddress') or [{}]
            for addr in addrs:
                defendants.append({
                    'case_number':    cn,
                    'defendant_name': _str(def_.get('defendantName')),
                    'race':           _str(def_.get('race')),
                    'gender':         _str(def_.get('gender')),
                    'dob':            _str(def_.get('dob')),
                    'height_feet':    def_.get('heightFeet'),
                    'height_inches':  def_.get('heightInches'),
                    'weight':         def_.get('weight'),
                    'address_line1':  _str(addr.get('addressLine1')),
                    'address_line2':  _str(addr.get('addressLine2')),
                    'city':           _str(addr.get('city')),
                    'state':          _str(addr.get('state')),
                    'zip':            _str(addr.get('zip')),
                    'current_address': (addr.get('currentAddress', '').lower() == 'yes')
                                       if addr.get('currentAddress') else None,
                })
            if not addrs or addrs == [{}]:
                defendants.append({
                    'case_number': cn, 'defendant_name': _str(def_.get('defendantName')),
                    'race': _str(def_.get('race')), 'gender': _str(def_.get('gender')),
                    'dob': _str(def_.get('dob')),
                    'height_feet': None, 'height_inches': None, 'weight': None,
                    'address_line1': None, 'address_line2': None,
                    'city': None, 'state': None, 'zip': None, 'current_address': None,
                })

        # ── Involved Parties + Attorneys ────────────────────────────
        parties   = []
        attorneys = []
        for p in (d.get('involvedParties') or []):
            for addr in (p.get('involvedPartyAddresses') or [{}]):
                parties.append({
                    'case_number':  cn,
                    'party_type':   _str(p.get('partyType')),
                    'party_name':   _str(p.get('partyName')),
                    'address_line1': _str(addr.get('addressLine1')),
                    'address_line2': _str(addr.get('addressLine2')),
                    'city':  _str(addr.get('city')),
                    'state': _str(addr.get('state')),
                    'zip':   _str(addr.get('zip')),
                })
            for atty in (p.get('attorneyInfo') or []):
                aa = (atty.get('attorneyAddress') or [{}])[0]
                attorneys.append({
                    'case_number':    cn,
                    'party_name':     _str(p.get('partyName')),
                    'attorney_name':  _str(atty.get('attorneyName')),
                    'appearance_date': _parse_date(atty.get('appearanceDate')),
                    'address_line1':  _str(aa.get('addressLine1')),
                    'city':  _str(aa.get('city')),
                    'state': _str(aa.get('state')),
                    'zip':   _str(aa.get('zip')),
                })

        # ── Charges (traffic/criminal) ──────────────────────────────
        charges = []
        for ch in (d.get('charges') or []):
            veh  = ch.get('vehicle') or {}
            stat = ch.get('statutes') or {}
            disp = ch.get('disposition') or {}
            charges.append({
                'case_number':       cn,
                'charge_number':     ch.get('chargeNumber'),
                'statute_code':      _str(stat.get('statuteCode')),
                'charge_description': _str(stat.get('chargeDescription')),
                'offense_date':      _parse_date(ch.get('chargeOffenseBeginDate')),
                'fine_amount':       ch.get('fineAmount'),
                'officer_name':      _str(ch.get('officerName')),
                'agency_name':       _str(ch.get('agencyName')),
                'vehicle_tag':       _str(veh.get('rgstrtnNmbr')),
                'vehicle_desc':      _str(veh.get('vehcileDescription')),
                'recorded_speed':    ch.get('recordedSpeed'),
                'speed_limit':       ch.get('speedLimit'),
                'mandatory_court':   _str(ch.get('mandatoryCourtAppearance')),
                'probable_cause':    _str(ch.get('probableCause')),
                'disposition':       _str(disp.get('dispositionType')),
            })

        # ── Hearings ────────────────────────────────────────────────
        hearings = []
        for h in (d.get('hearing') or []):
            hearings.append({
                'case_number': cn,
                'event_type':  _str(h.get('eventType')),
                'event_date':  _parse_date(h.get('eventDate')),
                'event_time':  _str(h.get('eventTime')),
                'location':    _str(h.get('location')),
                'room':        _str(h.get('room')),
                'result':      _str(h.get('result')),
                'judge':       _str(h.get('judge') or h.get('judgeId')),
            })

        # ── Case Events (documents) ─────────────────────────────────
        events = []
        for e in (d.get('caseEventInfo') or []):
            eid = e.get('internalEventID')
            events.append({
                'case_number':     cn,
                'file_date':       _parse_date(e.get('fileDate')),
                'document_name':   _str(e.get('documentName')),
                'internal_event_id': int(eid) if eid else None,
            })

        # ── Judgments ───────────────────────────────────────────────
        judgments = []
        for j in (d.get('judgmentEventInfo') or []):
            judgments.append({
                'case_number':   cn,
                'judgment_type': _str(j.get('judgmentEventType')),
                'issue_date':    _parse_date(j.get('issueDate')),
                'judge':         _str(j.get('judge')),
            })

        # ── Service Events ──────────────────────────────────────────
        service_events = []
        for s in (d.get('serviceEvents') or []):
            service_events.append({
                'case_number':  cn,
                'service_type': _str(s.get('serviceEventType')),
                'issue_date':   _parse_date(s.get('issueDate')),
            })

        # ── Causes of Action (civil) ────────────────────────────────
        causes = []
        for ca in (d.get('causesOfAction') or d.get('causeInfo') or []):
            rems = ca.get('remedyDTO') or ca.get('remedies') or []
            if rems:
                for rem in rems:
                    causes.append({
                        'case_number':       cn,
                        'file_date':         _parse_date(ca.get('fileDate')),
                        'filed_by':          _str(ca.get('filedBy')),
                        'filed_against':     _str(ca.get('filedAgainst')),
                        'cause_description': _str(ca.get('causeDescription') or ca.get('claimDescription')),
                        'remedy':            _str(rem.get('remedy') or rem.get('remedyType')),
                        'remedy_amount':     rem.get('remedyAmount') or rem.get('amount'),
                        'remedy_comment':    _str(rem.get('remedyComment') or rem.get('comment')),
                    })
            else:
                causes.append({
                    'case_number': cn,
                    'file_date': _parse_date(ca.get('fileDate')),
                    'filed_by': _str(ca.get('filedBy')),
                    'filed_against': _str(ca.get('filedAgainst')),
                    'cause_description': _str(ca.get('causeDescription') or ca.get('claimDescription')),
                    'remedy': None, 'remedy_amount': None, 'remedy_comment': None,
                })

        _db_write(cn, main, defendants, parties, attorneys,
                  charges, hearings, events, judgments, service_events, causes)

        logger.debug(
            f'MJCS2: {cn} — {len(defendants)}defs {len(parties)}parties '
            f'{len(attorneys)}attys {len(charges)}charges {len(hearings)}hearings '
            f'{len(events)}events {len(judgments)}judgments '
            f'{len(service_events)}svc {len(causes)}causes'
        )

    # ══════════════════════════════════════════════════════════════
    # DOMESTIC VIOLENCE  dvCaseDTO format
    # ══════════════════════════════════════════════════════════════
    def _parse_dv(self, dv):
        cn = self.case_number
        filing_date_str = _str(dv.get('filingDate', ''))

        main = {
            'case_number':       cn,
            'internal_id':       _str(dv.get('caseIdx')),
            'court_system':      _str(dv.get('courtSystem')),
            'case_category':     'DV',
            'case_type':         _str(dv.get('caseType', 'DOMESTIC VIOLENCE')),
            'case_title':        None,
            'filing_date':       _parse_date(filing_date_str),
            'filing_date_str':   filing_date_str,
            'case_status':       _str(dv.get('caseStatus')),
            'case_status_date':  None,
            'court_name':        None,
            'judge_assigned':    None,
            'charge_track_number': None,
            'violation_date':    None,
            'violation_county':  None,
        }

        defendants = []
        parties    = []
        for p in (dv.get('partyDTO') or []):
            addr = (p.get('partyAddress') or [{}])[0]
            parties.append({
                'case_number':   cn,
                'party_type':    _str(p.get('partyType')),
                'party_name':    _str(p.get('defendantName')),
                'address_line1': _str(addr.get('addressLine1')),
                'address_line2': _str(addr.get('addressLine2')),
                'city':  _str(addr.get('city')),
                'state': _str(addr.get('state')),
                'zip':   _str(addr.get('zip')),
            })
            if p.get('partyType') == 'RES':      # RES = Respondent = defendant
                defendants.append({
                    'case_number':    cn,
                    'defendant_name': _str(p.get('defendantName')),
                    'race': None, 'gender': None,
                    'dob':  _str(p.get('dob')),
                    'height_feet': None, 'height_inches': None, 'weight': None,
                    'address_line1': _str(addr.get('addressLine1')),
                    'address_line2': _str(addr.get('addressLine2')),
                    'city':  _str(addr.get('city')),
                    'state': _str(addr.get('state')),
                    'zip':   _str(addr.get('zip')),
                    'current_address': None,
                })

        hearings = []
        for h in (dv.get('hearingDTO') or []):
            hearings.append({
                'case_number': cn,
                'event_type':  _str(h.get('eventType')),
                'event_date':  _parse_date(h.get('eventDate')),
                'event_time':  _str(h.get('eventTime')),
                'location':    _str(h.get('location')),
                'room':        _str(h.get('room')),
                'result':      _str(h.get('result')),
                'judge':       None,
            })

        events = []
        for e in (dv.get('otherEvents') or []):
            events.append({
                'case_number':     cn,
                'file_date':       _parse_date(e.get('eventDate')),
                'document_name':   _str(e.get('eventType')),
                'internal_event_id': None,
            })

        _db_write(cn, main, defendants, parties, [], [], hearings, events, [], [], [])
        logger.debug(f'MJCS2(DV): {cn} — {len(defendants)}defs {len(hearings)}hearings')
