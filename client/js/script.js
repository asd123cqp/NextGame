var server = 'http://nextgame-api.cqp.cloud'
var head = {
  headers: {
    'Content-Type': 'application/json',
  },
  method: 'GET'
};

function createNode(type, classes) {
  var node = document.createElement(type);
  classes.forEach(function(c) {
    node.classList.add(c);
  });
  return node;
}

function getKeyword() {
  var kw = document.getElementById('search-form').elements.kw.value;
  return kw === '' ? '0' : kw;
}

function getType() {
  return document.getElementById('search-form').elements.type.value;
}

function composeQuery() {
  return `/${getType()}/${getKeyword()}`;
}

function showGames() {
  fetch(server + composeQuery(), head)
    .then(function (res) {
      return res.json();
    }).then(function (data) {
      renderAllActivities(data, getType());
      return false;
    }).catch(function (error) {
      console.log(error);
  });
}

function buyGame(gid){
  fetch(server + `/buy/${getKeyword()}/${gid}`, head)
    .then(function (res) {
      return res.json();
    }).then(function (data) {
      alert(data.result)
      var btn = document.getElementById(`Buy-${gid}-btn`);
      btn.classList.toggle('btn-success');
      btn.classList.toggle('btn-danger');
      btn.onclick = '';
      // showGames();
      return false;
    }).catch(function (error) {
      console.log(error);
  });
}

function playGame(gid) {

  fetch(server + `/play/${getKeyword()}/${gid}`, head)
    .then(function (res) {
      return res.json();
    }).then(function (data) {
      alert(data.result)
      return false;
    }).catch(function (error) {
      console.log(error);
  });
}

function renderAllActivities(activities, type) {
  var frame = document.getElementById('activities-frame');
  while (frame.hasChildNodes()) {
    frame.removeChild(frame.lastChild);
  }

  var buttonText = type === 'library' ? 'Play' : 'Buy';
  activities.forEach(function(activity) {
    var buttonCallback = type === 'library' ? 'playGame' : 'buyGame';
    frame.appendChild(renderActivity(activity, buttonText, buttonCallback));
  });
}

function renderActivity(activity, buttonText, buttonCallback) {
  var node = createNode('div', ['card', 'text-center']);
  node.innerHTML = `
<img class="activity-img card-img-top" src="${activity.HeaderImage}">
<div class="card-block activity-info">
  <h4 class="card-title activity-name">${activity.Name}</h4>
  <p class="card-text activity-explanation">${activity.ShortDescrip}</p>
  <button class="btn btn-success" id="${buttonText}-${activity.ID}-btn" onclick="${buttonCallback}('${activity.ID}')">${buttonText}</button>
  <p></p>
</div>

<div class="card-footer text-muted">
  <i class="fa ${buttonText === 'Buy' ? 'fa-usd' : 'fa-clock-o'}" aria-hidden="true"></i> ${buttonText === 'Buy' ? activity.PriceFinal : activity.PlayTime + ' min'}
</div>
  `;
  return node;
}

/******************** login form ********************/
$('.form').find('input, textarea').on('keyup blur focus', function (e) {

  var $this = $(this),
      label = $this.prev('label');

    if (e.type === 'keyup') {
      if ($this.val() === '') {
          label.removeClass('active highlight');
        } else {
          label.addClass('active highlight');
        }
    } else if (e.type === 'blur') {
      if( $this.val() === '' ) {
        label.removeClass('active highlight');
      } else {
        label.removeClass('highlight');
      }
    } else if (e.type === 'focus') {

      if( $this.val() === '' ) {
        label.removeClass('highlight');
      }
      else if( $this.val() !== '' ) {
        label.addClass('highlight');
      }
    }

});

function tabClick(e) {

  e.preventDefault();

  $(this).parent().addClass('active');
  $(this).parent().siblings().removeClass('active');

  target = $(this).attr('href');

  $('.tab-content > div').not(target).hide();

  $(target).fadeIn(600);

}


$('.tab a').on('click', tabClick);
/******************** login form ********************/
