def test_valid(cldf_dataset, cldf_logger):
    assert cldf_dataset.validate(log=cldf_logger)


def test_forms(cldf_dataset):
    assert len(list(cldf_dataset["FormTable"])) == 21967
    assert any(f["Form"] == "tɑ³⁵pʐɑ³⁵n̥e³⁵" for f in cldf_dataset["FormTable"])
    assert any(f["Form"] == "vɛ⁵⁵" for f in cldf_dataset["FormTable"])


def test_parameters(cldf_dataset):
    assert len(list(cldf_dataset["ParameterTable"])) == 883


def test_languages(cldf_dataset):
    assert len(list(cldf_dataset["LanguageTable"])) == 25
