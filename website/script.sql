 create database eFactura;
 use efactura;

CREATE TABLE IF NOT EXISTS trimitereFacturi (
    id INT AUTO_INCREMENT PRIMARY KEY,
    Factura INT,
    index_incarcare bigint
);


ALTER TABLE trimitereFacturi
ADD INDEX idx_index (index_incarcare);

ALTER TABLE trimitereFacturi
ADD column data_trimis TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- am valoare unica pe id_factura
-- daca de exemplu dau un insert de mai multe ori, asa evit sa mi se dubleze datele
ALTER TABLE trimitereFacturi
ADD CONSTRAINT uc_index_incarcare UNIQUE (index_incarcare);


CREATE TABLE IF NOT EXISTS statusMesaje (
    id INT AUTO_INCREMENT PRIMARY KEY,
    data_creare bigint,
    cif VARCHAR(255),
    id_solicitare varchar(255),
    detalii VARCHAR(500),
    tip VARCHAR(255),
    id_factura bigint,
    FOREIGN KEY (id_solicitare) REFERENCES trimitereFacturi(index_incarcare)
);
drop table statusMesaje;
-- am valoare unica pe id_factura
-- daca de exemplu dau un insert de mai multe ori, asa evit sa mi se dubleze datele
ALTER TABLE statusMesaje
ADD CONSTRAINT uc_id_solicitare UNIQUE (id_factura);

select * from trimiterefacturi join statusmesaje
on trimiterefacturi.index_incarcare = statusmesaje.id_solicitare;

select count(*) from trimiterefacturi;
select count(*) from statusmesaje;
select * from trimiterefacturi;
select * from statusmesaje;

delete from statusMesaje;
delete from trimiterefacturi;

set sql_Safe_updates = 0;

alter view JOINDATE as
select factura, data_creare, cif, id_solicitare, detalii, tip, id_factura
from trimiterefacturi join statusmesaje
on trimiterefacturi.index_incarcare = statusmesaje.id_solicitare;

select * from JOINDATE;

select * from trimiterefacturi;
select count(*) from joindate;
select count(*) from trimiterefacturi;
select count(*) from statusmesaje;

SELECT *
FROM statusmesaje
WHERE detalii LIKE '%erori%';

ALTER TABLE statusmesaje
MODIFY COLUMN id_solicitare VARCHAR(255);


ALTER TABLE trimiterefacturi
MODIFY COLUMN index_incarcare VARCHAR(255);

alter table statusmesaje
drop foreign key statusmesaje_ibfk_1;

truncate trimiterefacturi;

select * from trimiterefacturi where index_incarcare = 5004087578;

select * from trimiterefacturi;
select * from statusmesaje;

CREATE TABLE ultimele_facturi_trimise (
    id_factura INT AUTO_INCREMENT PRIMARY KEY,
    numar_factura VARCHAR(255) NOT NULL,
    data_trimis TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

drop table ultimele_facturi_trimise;

SELECT data_trimis, COUNT(*) AS numar_date
FROM trimiterefacturi
GROUP BY data_trimis
HAVING COUNT(*) > 1
order by data_trimis desc
limit 1;

select * from trimiterefacturi
order by data_trimis desc;

CREATE TABLE FisierePDF (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nume_fisier VARCHAR(255),
    continut BLOB,
    FOREIGN KEY (nume_fisier) REFERENCES statusMesaje(id_solicitare)
);
alter table fisierepdf
drop foreign key fisierepdf_ibfk_1;

select * from FisierePDF;
delete from fisierepdf;
truncate fisierepdf;

set sql_Safe_updates = 1;

select * from statusmesaje join fisierepdf
on statusmesaje.id_solicitare = fisierepdf.nume_fisier
where tip="FACTURA PRIMITA";

create view tabelaFisierePDF as 
select nume_fisier, continut from statusmesaje join fisierepdf
on statusmesaje.id_solicitare = fisierepdf.nume_fisier
where tip="FACTURA TRIMISA";

CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(100) NOT NULL,
    password VARCHAR(1000) NOT NULL,
    token VARCHAR(100) NOT NULL,
    
);

-- select * from users;
-- delete from users where id between 1 and 7;
-- drop table users;

alter table trimitereFacturi
add column user_id int;

alter table users
add column trimiterefact int;

SET GLOBAL wait_timeout = 50800; -- or any other suitable value
SET GLOBAL interactive_timeout = 50800;

alter table trimiterefacturi
add foreign key (user_id) references users(id);