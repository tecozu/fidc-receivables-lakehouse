import pytest
from faker import Faker

from fidc_lakehouse.documents import is_valid_cnpj, is_valid_cpf, is_valid_document

fake = Faker("pt_BR")
fake.seed_instance(0)


@pytest.mark.parametrize("cpf", [fake.cpf() for _ in range(20)])
def test_generated_cpfs_are_valid(cpf):
    assert is_valid_cpf(cpf)
    assert is_valid_document(cpf)


@pytest.mark.parametrize("cnpj", [fake.cnpj() for _ in range(20)])
def test_generated_cnpjs_are_valid(cnpj):
    assert is_valid_cnpj(cnpj)
    assert is_valid_document(cnpj)


@pytest.mark.parametrize(
    "value",
    ["", "123", "111.111.111-11", "00.000.000/0000-00", "12.ABC.345/0001-XX", "529.982.247-26"],
)
def test_invalid_documents(value):
    assert not is_valid_document(value)


def test_check_digit_tampering_is_detected():
    cnpj = fake.cnpj()
    last = int(cnpj[-1])
    tampered = cnpj[:-1] + str((last + 1) % 10)
    assert not is_valid_cnpj(tampered)
