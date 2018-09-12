# coding=utf-8
from __future__ import unicode_literals, print_function

from clldutils.path import Path
import lingpy as lp

from clldutils.path import Path
from pylexibank.dataset import Metadata
from pylexibank.dataset import Dataset as BaseDataset
from pylexibank.lingpy_util import getEvoBibAsBibtex


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
    DSETS = {
        'SLV.csv': 'Starostin2005b',
        'SIN.csv': 'Hou2004',
        'ROM.csv': 'Starostin2005b',
        'PIE.csv': 'Starostin2005b',
        'PAN.csv': 'Greenhill2008',
        'OUG.csv': 'Zhivlov2011',
        'KSL.csv': 'Kessler2001',
        'JAP.csv': 'Hattori1973',
        'IEL.csv': 'Dunn2012',
        'IDS.csv': 'List2014c',
        'GER.csv': 'Starostin2005',
        'BAI.csv': 'Wang2006',
    }

    def cmd_download(self, **kw):
        d = Path('SequenceComparison-SupplementaryMaterial-cc4bf85/benchmark/cognates/')
        self.raw.download_and_unpack(
            self.metadata.url,
            *[d.joinpath(dset) for dset in self.DSETS],
            **{'log': self.log})
        self.raw.write('sources.bib', getEvoBibAsBibtex(*set(self.DSETS.values()), **kw))

    def cmd_install(self, **kw):
        gloss2con = {x['GLOSS']: x['CONCEPTICON_ID'] for x in self.concepts}
        lang2glot = {x['NAME']: x['GLOTTOCODE'] for x in self.languages}

        with self.cldf as ds:
            for dset, srckey in self.DSETS.items():
                wl = lp.Wordlist(self.raw.posix(dset))
                if 'tokens' not in wl.header:
                    wl.add_entries(
                        'tokens',
                        'ipa',
                        lp.ipa2tokens,
                        merge_vowels=False,
                        expand_nasals=True)

                ds.add_sources(*self.raw.read_bib())
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

                    ds.add_language(
                        ID=doculect,
                        name=wl[k, 'doculect'],
                        glottocode=lang2glot[doculect])
                    ds.add_concept(
                        ID=wl[k, 'concept'],
                        gloss=wl[k, 'concept'],
                        conceptset=gloss2con.get(wl[k, 'concept'], ''))
                    for row in ds.add_lexemes(
                        Language_ID=doculect,
                        Parameter_ID=wl[k, 'concept'],
                        Value=wl[k, 'ipa'],
                        Source=[srckey],
                        Segments=wl[k, 'tokens'] or [],
                        Cognacy=cogid,
                        Loan=wl[k, 'loan']
                    ):
                        cognates.append(ds.add_cognate(
                            lexeme=row,
                            Index=k,
                            Cognateset_ID=cogid,
                            Cognate_detection_method='expert',
                            Doubt=loan,
                            Cognate_source=srckey))

                ds.align_cognates(lp.Alignments(wl), cognates, method='library')
                for er in sorted(set(errors)):
                    self.log.debug(er, dset)
