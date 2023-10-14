const { Client } = require('@notionhq/client');
const {BigQuery} = require('@google-cloud/bigquery');
const dotenv = require('dotenv');

dotenv.config();
const notion = new Client({ auth: process.env.API_KEY });
const bigquery = new BigQuery({projectId: process.env.BIGQUERY_PROJECT});

let users;
const rows = [];


const main = async function () {
  console.log('Starting to read data from Notion');
  console.log('Retrieving users')
  await getUsers();
  console.log(`${users.length} users retrieved`)
  console.log('Retrieving pages')
  await getPages();
  console.log(`${rows.length} pages retrieved`)
  console.log('Starting BigQuery work')
  // await insertInBigQuery();
}

// Get all users from notion to be able to add username to the report
const getUsers = async function () {
  const response = await notion.users.list();
  users = response.results;
};

const getPages = async function () {
  const response = await notion.search({
    sort: {
      direction: 'ascending',
      timestamp: 'last_edited_time'
    },
  });
  const results = response.results;
  results.forEach((result) => {
    if (result.properties.Verification && result.properties.Verification.verification.state) {

      const props = [];
      for (const key in result.properties) {
        props.push({
          name: key,
          value: result.properties[key]
        })
      }
        
      let pageData = {
          created_time: result.created_time,
          last_edited_time: result.last_edited_time,
          url: result.url,
          properties: props.map(x => JSON.stringify(x)),
      }
      rows.push(pageData);
    }
  });
}

const insertInBigQuery = async function () {
  await deleteTable();
  console.log('Table deleted')
  await littlePause();
  const schema = [
      { name: 'created_time', type: 'STRING' },
      { name: 'last_edited_time', type: 'STRING' },
      { name: 'url', type: 'STRING' },
      { name: 'properties', type: 'STRING', mode: 'REPEATED' },
  ]

  const options = {
    schema: schema,
  };


  // Create a new table in the dataset
  const [table] = await bigquery
    .dataset(process.env.BIGQUERY_DATASET)
    .createTable(process.env.BIGQUERY_TABLE, options);
  console.log(`Table ${table.id} created.`);

  let insertStatus = false;
  // Retying inserts as it fails sometimes when table is not available yet
  while (insertStatus === false){
    try {
      console.log('Inserting rows...');
      insertStatus = await insertRows();
    } catch(e) {
      await littlePause(2000);
    }
  }

}

const insertRows = async function () {
  try  {
    await bigquery
    .dataset(process.env.BIGQUERY_DATASET)
    .table(process.env.BIGQUERY_TABLE)
    .insert(rows);
  } catch(e) {
    console.log('Errors:');
    e.errors.forEach(error => {
      console.log(error.message);
    });
    return false
  }
  console.log('Success');
  return true;
}

const littlePause = async function(time = 2000) {
  return new Promise(resolve => {
    setTimeout(() => {
      resolve();
    }, time);
  });
}

const deleteTable = async function () {
  try {
    await bigquery
      .dataset(process.env.BIGQUERY_DATASET)
      .table(process.env.BIGQUERY_TABLE)
      .delete(); 
  } catch (error) {
    // console.log(error);
  }
}

main();