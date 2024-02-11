const canvas = document.getElementById("vectorField");
const ctx = canvas.getContext("2d");

// Set the z-index of the canvas
canvas.style.zIndex = '1'; // Adjust the z-index as needed
// Set canvas size to match map container size
canvas.width = map.getSize().x;
canvas.height = map.getSize().y;
console.log(map.getSize().x)
console.log(ctx);
ctx.fillStyle = 'black';
ctx.strokeStyle = 'white';
ctx.lineWidth = 1;

class Particle{
    constructor(effect){
        this.effect = effect;
        this.x = Math.floor(Math.random()* this.effect.width);
        this.y = Math.floor(Math.random()* this.effect.height);
        this.speedX = Math.random()* 5 -2.5;
        this.speedY = Math.random()* 5 -2.5;
        this.history = [{x:this.x, y:this.y}]
        this.maxLength = 30;
        this.angle = 0;
    }
    draw(context){
        context.fillRect(this.x, this.y, 10, 10)
        context.beginPath();
        context.moveTo(this.history[0].x, this.history[0].y);
        for (let i = 0; i < this.history.length; i++){
            context.lineTo(this.history[i].x, this.history[i].y);
        }
        context.stroke();
    }
    update(){

        this.x += this.speedX;
        this.y += this.speedY;
        this.history.push({x: this.x, y: this.y});
        // If the length of the particle is greater than its max length property then shift array element
        if (this.history.length > this.maxLength){
            //shift array method removes one element from the beginning of an array
            this.history.shift();
        }
        console.log(this.x & this.y)
    }
}

class Effect {
    constructor(width,height){
        this.width = width;
        this.height = height;
        this.particles = [];
        this.numberOfParticles = 50;
        this.cellSize = 20;
        this.rows;
        this.cols;
        this.flowField = [];
        this.init();
    }
    init(){
        //create flow field
        this.rows = Math.floor(this.height/ this.cellSize);
        this.cols = Math.floor(this.width/ this.cellSize);
        this.flowField = [];
        for (let y=0; y < this.rows; y++){
            for (let x=0; x < this.cols; x++){
                let angle = Math.cos(x) + Math.sin(y);
                this.flowField.push(angle);
            }
        }
        // create particles
        for (let i = 0;i <this.numberOfParticles; i++){
        this.particles.push(new Particle(this))};
    }
    render(context){
        this.particles.forEach(particle=>{
            particle.draw(context);
            particle.update();
        })
    }
}

const effect = new Effect(canvas.width , canvas.height);

effect.render(ctx);
// Generate an animation loop where render method indefinitely in loop as function continually calls itself
function animate(){
    ctx.clearRect(0,0,canvas.width, canvas.height);
    effect.render(ctx);
    requestAnimationFrame(animate);
}
animate();