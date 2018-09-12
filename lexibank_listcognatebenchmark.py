# coding=utf-8
from __future__ import unicode_literals, print_function

from clldutils.path import Path
import lingpy as lp

from clldutils.path import Path
from clldutils.misc import slug
from pylexibank.dataset import Metadata
from pylexibank.dataset import Dataset as BaseDataset
from pylexibank.util import getEvoBibAsBibtex


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

    def cmd_download(self, **kw):
        d = Path('SequenceComparison-SupplementaryMaterial-cc4bf85/benchmark/cognates/')
        self.raw.download_and_unpack(
            self.metadata.url,
            *[d.joinpath(dset) for dset in self.DSETS],
            **{'log': self.log})
        self.raw.write('sources.bib', getEvoBibAsBibtex(*set(v[0] for v in self.DSETS.values()), **kw))

    def cmd_install(self, **kw):
        gloss2con = {x['GLOSS']: x['CONCEPTICON_ID'] for x in self.concepts}

        with self.cldf as ds:
            ds.add_languages(id_factory=lambda l: slug(l['Name'], lowercase=False))
            for dset, (srckey, col) in sorted(self.DSETS.items()):
                try:
                    wl = lp.Wordlist(str(self.raw / dset), col=col or 'doculect')
                except:
                    print(dset)
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

                ds.add_sources()
                errors = []
                cognates = []
                for k in wl:
                    concept = wl[k, 'concept']
                    if '(V)' in concept:
                        concept = concept[:-4]
                    concept = correct_concepts.get(concept, concept)
                    if concept not in gloss2con:
                        errors += [concept]
                    doculect = correct_languages.get(wl[k, 'doculect'], wl[k, 'doculect'])
                    loan = wl[k, 'cogid'] < 0
                    cogid = abs(wl[k, 'cogid'])

                    ds.add_concept(
                        ID=slug(wl[k, 'concept'], lowercase=False),
                        Name=wl[k, 'concept'],
                        Concepticon_ID=gloss2con.get(wl[k, 'concept'], ''))
                    for row in ds.add_lexemes(
                        Language_ID=slug(doculect, lowercase=False),
                        Parameter_ID=slug(wl[k, 'concept'], lowercase=False),
                        Value=wl[k, 'ipa'],
                        Source=[srckey],
                        Segments=wl[k, 'tokens'] or [],
                        Cognacy=cogid,
                        Loan=wl[k, 'loan']
                    ):
                        cognates.append(ds.add_cognate(
                            lexeme=row,
                            ID=k,
                            Cognateset_ID=cogid,
                            Cognate_Detection_Method='expert',
                            Doubt=loan,
                            Source=[srckey]))

                ds.align_cognates(lp.Alignments(wl), cognates, method='library')
                for er in sorted(set(errors)):
                    self.log.debug(er, dset)
