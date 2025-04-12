<template>
  <div class="home flex flex-col items-center">
    <!-- Games Wrapper -->
    <div class="flex flex-wrap justify-center gap-6 w-full max-w-4xl">
      <!-- Game Card for Pet Name Guessing -->
      <div class="game-card bg-[var(--secondary-light)] p-6 rounded-xl shadow-md hover:shadow-lg transition max-w-md w-full">
        <h3 class="text-xl font-bold mb-2 text-center">Petfinder Name Game</h3>
        <p class="mb-4 text-center">Guess the correct names of these adorable pets?</p>
        <router-link 
          to="/games/pet-name-game" 
          class="block text-center py-2 px-4 bg-[var(--primary-purple)] text-white rounded hover:bg-[var(--secondary-purple)]"
        >
          Play Now
        </router-link>
      </div>

      <!-- Additional Game Cards -->
      <!-- <div class="game-card bg-white p-6 rounded-xl shadow-md hover:shadow-lg transition max-w-md w-full">
        <h3 class="text-xl font-bold mb-2 text-center">Pet Breed Quiz</h3>
        <p class="mb-4 text-center">Test your knowledge of different pet breeds!</p>
        <router-link 
          to="/games/breed-quiz" 
          class="block text-center py-2 px-4 bg-blue-500 text-white rounded hover:bg-blue-600 transition"
        >
          Coming Soon
        </router-link>
      </div> -->
    </div>
  </div>
</template>

<script>
export default {
  name: 'Home'
}
</script>
