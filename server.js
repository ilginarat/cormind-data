const express = require("express");
const bodyParser = require("body-parser");
const request = require("request");
const fs = require('fs');
const { parse } = require("csv-parse");
const multer = require('multer');


const app = express();
const upload = multer({ dest: 'uploads/' });
const { Client } = require('pg');

// Create a PostgreSQL connection pool
const client = new Client({
    user: 'ilginarat',
    host: 'localhost', 
    database: 'mytest',
    password: 'Merhaba1',
    port: 5432, //default port for postgresql
  });


client.connect()
  .then(() => console.log('Connected to PostgreSQL database'))
  .catch(err => console.error('Error connecting to PostgreSQL database', err));
  //connected to postgresql


app.get('/', (req, res) => {

    //console.log("ee hacı");
    const createTableProduct = `
    CREATE TABLE IF NOT EXISTS products (
        id BIGSERIAL NOT NULL PRIMARY KEY,
        stock_no VARCHAR(50),
        product_name VARCHAR(50),
        product_code VARCHAR(50),
        UNIQUE(product_code)
    )
    `;
    client.query(createTableProduct)
    .then(() => {
        console.log('Table created successfully');
        res.write('Table created successfully1');
    })
    .catch(err => {
        console.error('Error creating table', err);
        res.write('Error creating table');
    });


    //YARATTIĞIM İÇİN SİLMEK GEREKEBİLİR
    const createTableProductPiece = `   
    CREATE TABLE IF NOT EXISTS productPiece (
        id BIGSERIAL NOT NULL PRIMARY KEY,
        part_code VARCHAR(50),
        part_name VARCHAR(50),
        thickness VARCHAR(5),
        width VARCHAR(5),
        height VARCHAR(6),
        amount VARCHAR(6),
        product_id BIGINT REFERENCES products(id)
    )
    `;
    client.query(createTableProductPiece)
    .then(() => {
        console.log('Table created successfully');
        res.write('Table created successfully2');
    })
    .catch(err => {
        console.error('Error creating table', err);
        res.write('Error creating table');
    });
//tables are created successfully

    const results = [];

    fs.createReadStream(__dirname + '/ProductParts_REV002.csv')
    .pipe(parse({ delimiter: ";", from_line: 2 }))
    .on('data', (data) => {
      //console.log(data);  //datalar geliyor
      results.push(data);
    })
    .on('end', () => {

      //console.log(results); //2 key olarak ayrılmış her obje

      const insertQueryProduct = `
        INSERT INTO products (stock_no, product_name, product_code)
        VALUES ($1, $2, $3)
        ON CONFLICT(product_code) DO NOTHING
      `;

      const insertQueryProductPiece = `
        INSERT INTO productPiece (part_code, part_name, thickness, width, height, amount, product_id)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
      `;

      const findIdQuery = `
        SELECT id FROM products WHERE product_code = $1
      `;

      client.query('BEGIN')

        .then(() => {
          const insertions = results.map((result) => {

            //console.log("heeeyo");  map fonksiyonunun içine giriyor

            const [productCode, stockName, stockNo, pieceCode, pieceName, thickness, width, height, amount] = result; //object destructing

            //console.log(productCode); //object destructing calisiyor

            const query1 = client.query(insertQueryProduct, [stockNo, stockName, productCode]);
            //const query2 = client.query(insertQueryProductPiece, [pieceCode, pieceName, thickness, width, height, amount, stockNo]);

            return query1;

          });
          // Use Promise.all() to wait for all promises to resolve
          return Promise.all(insertions);
        })
        .then(() => {
            //productpiece table insertions
            const insertions = results.map( (result) => {

              //console.log("ilk insertion okay.");

              const [productCode, stockName, stockNo, pieceCode, pieceName, thickness, width, height, amount] = result; //object destructing

              const query_1 =  client.query(findIdQuery, [productCode])  //gives the product id
                .then((product_id) => {
                  //console.log(product_id);  direkt id value sunu vermiyor
                  const productID = product_id.rows[0].id;  //NIYE?????????????
                  //console.log(productID);
                  const query_2 = client.query(insertQueryProductPiece, [pieceCode, pieceName, thickness, width, height, amount, productID]);
                  return query_2;
                });
              return query_1;

            });
            return Promise.all(insertions);
        })
        .then(() => {
          return client.query('COMMIT');
        })
        .then(() => {
          console.log('Data inserted successfully');
          res.write('Data inserted successfully');
          res.end();
        })
        .catch((err) => {
          client.query('ROLLBACK')
            .then(() => {
              console.error('Error inserting data', err);
              res.write('Error inserting data');
              res.end();
            })
            .catch((err) => {
              console.error('Error rolling back transaction', err);
              res.write('Error rolling back transaction');
              res.end();
            });
        })
    });
});


  process.on('SIGINT', () => {
    console.log('Shutting down server');
    client.end()
      .then(() => console.log('Disconnected from PostgreSQL database'))
      .catch(err => console.error('Error disconnecting from PostgreSQL database', err))
      .finally(() => process.exit());
  });


app.listen(3032, function () {
    console.log("server is on port 3031");
});