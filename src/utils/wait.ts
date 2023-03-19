const wait = async ({ seconds = 0 }) =>
  new Promise((resolve) => {
    setTimeout(resolve, seconds * 1000);
  });

export default wait;
