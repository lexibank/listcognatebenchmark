# coding=utf-8
from __future__ import unicode_literals, print_function

from pathlib import Path
import lingpy as lp

from clldutils.path import Path
from clldutils.misc import slug
from pylexibank import Dataset as BaseDataset
from pylexibank.util import getEvoBibAsBibtex
from pylexibank import progressbar


correct_languages = {
    "Guixian": "Guiyang",
    "Berawan (Long Terawan)": "Berawan_Long_Terawan",
    "Merina (Malagasy)": "Merina_Malagasy"
}
correct_concepts = {
    "ear 1": "ear",
    "i": "I",
    "lie 1": "lie",
    "light": "watery",
    "soja sauce": "soya sauce",
    "two pairs": "two ounces",
    "walk (go)": "walk(go)",
    "warm (hot)": "warm",
    "gras": "grass",
    "saliva (splits)": "saliva (spit)"
}

class Dataset(BaseDataset):
    dir = Path(__file__).parent
    id = 'listcognatebenchmark'
    DSETS = {
        'SLV.csv': ('Starostin2005b', None),
        'SIN.csv': ('Hou2004', 'languages'),
        'ROM.csv': ('Starostin2005b', None),
        'PIE.csv': ('Starostin2005b', None),
        'PAN.csv': ('Greenhill2008', None),
        'OUG.csv': ('Zhivlov2011', None),
        'KSL.csv': ('Kessler2001', None),
        'JAP.csv': ('Hattori1973', None),
        'IEL.csv': ('Dunn2012', None),
        'IDS.csv': ('List2014c', None),
        'GER.csv': ('Starostin2005', None),
        'BAI.csv': ('Wang2006', None),
    }

    def cmd_download(self, args):
        d = Path('SequenceComparison-SupplementaryMaterial-cc4bf85/benchmark/cognates/')
        self.raw_dir.download_and_unpack(
            self.metadata.url,
            *[d.joinpath(dset) for dset in self.DSETS],
            **{'log': args.log})
        self.raw_dir.write('sources.bib', 
                getEvoBibAsBibtex(*set(v[0] for v in self.DSETS.values())))

    def cmd_makecldf(self, args):
    
        concepts = {}
        for concept in self.concepts:
            idx = '{0}_{1}'.format(concept['NUMBER'], slug(concept['ENGLISH']))
            args.writer.add_concept(
                    ID=idx,
                    Name=concept['ENGLISH'],
                    Concepticon_ID=concept['CONCEPTICON_ID']
                    )
            concepts[concept['ENGLISH']] = idx
        args.writer.add_languages(
                id_factory=lambda l: slug(l['Name'], lowercase=False)
                )
        args.writer.add_sources()
        for dset, (srckey, col) in progressbar(
                sorted(self.DSETS.items()),
                desc='adding datasets'):
            try:
                wl = lp.Wordlist(str(self.raw_dir / dset), col=col or 'doculect')
            except:
                args.log.warn('missing datasets {0}'.format(dset))
                raise

            if 'doculect' not in wl.header:
                wl.add_entries('doculect', col, lambda i: i)
            if 'tokens' not in wl.header:
                wl.add_entries(
                    'tokens',
                    'ipa',
                    lp.ipa2tokens,
                    merge_vowels=False,
                    expand_nasals=True)

            errors = []
            cognates = []
            for k in wl:
                concept = wl[k, 'concept']
                if '(V)' in concept:
                    concept = concept[:-4]
                concept = correct_concepts.get(concept, concept)
                if concept not in concepts:
                    errors += [concept]
                doculect = correct_languages.get(wl[k, 'doculect'], wl[k, 'doculect'])
                loan = wl[k, 'cogid'] < 0
                cogid = abs(wl[k, 'cogid'])
                
                row = args.writer.add_form_with_segments(
                    Language_ID=slug(doculect, lowercase=False),
                    Parameter_ID=concepts[concept],
                    Value=wl[k, 'ipa'],
                    Form=wl[k, 'ipa'],
                    Source=[srckey],
                    Segments=wl[k, 'tokens'] or [],
                    Cognacy=cogid,
                    Loan=wl[k, 'loan']
                    )
                args.writer.add_cognate(
                    lexeme=row,
                    ID=k,
                    Cognateset_ID=cogid,
                    Cognate_Detection_Method='expert',
                    Doubt=loan,
                    Source=[srckey])

            for er in sorted(set(errors)):
                self.log.debug(er, dset)
